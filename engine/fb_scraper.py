#!/usr/bin/env python3
"""
Authorized Facebook public-group collection -> local PocketBase

Changes in this version
  * EXTRACT_JS now returns a JSON string (fixes "cannot deserialize the result value
    received from Runtime.callFunctionOn" crashes on long feeds).
  * safe_process(): if an extract call fails with that error, retry once in place
    instead of restarting Chrome and re-scrolling from the top.
  * Everything else (hide processed posts, hard timeouts, Chrome restart, auto resume,
    real dates via hover tooltip, parallel workers) is unchanged.

Run (all groups, rolling last 30 days, fast, 2 workers):
  python fb_scraper_fast.py --days 30 --workers 2 --fast --export last_30_days.csv

Run only groups 10 through 12 (1-based, inclusive):
  python fb_scraper_fast.py --days 30 --start 10 --end 12 --workers 2 --fast

  --fast = shorter page/scroll waits and lower memory use. For maximum speed
           when image URLs are not needed, also pass --no-images.
  The log shows "posts/min" every 10 scrolls so you can see the real speed.

Group positions are 1-based and --end is inclusive. For example, --start 10 --end 12
processes rows 10, 11, and 12 from groups.csv.

Optional (frees more memory on very long feeds), PowerShell:
  $env:PRUNE_MODE="empty"

Only scrape groups/content you are authorized to access.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import queue
import random
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, unquote, urlparse

import requests
from selenium import webdriver
from selenium.common.exceptions import (
    ElementNotInteractableException,
    JavascriptException,
    MoveTargetOutOfBoundsException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

# ----------------------------- CONFIG
POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "http://127.0.0.1:8091").rstrip("/")
PB_EMAIL = os.environ.get("PB_EMAIL", "")
PB_PASSWORD = os.environ.get("PB_PASSWORD", "")
COOKIE_FILE = os.environ.get("COOKIE_FILE", "facebook-cookies.json")
GROUPS_CSV = os.environ.get("GROUPS_CSV", "groups.csv")
HEADLESS = os.environ.get("HEADLESS", "true").lower() == "true"
BLOCK_IMAGES = os.environ.get("BLOCK_IMAGES", "true").lower() == "true"
PRUNE_MODE = os.environ.get("PRUNE_MODE", "hide")  # "hide" (safe) or "empty" (frees more memory)
SAFE_MODE = os.environ.get("SAFE_MODE", "false").lower() == "true"   # old-script-like browser settings
PAGE_LOAD = os.environ.get("PAGE_LOAD", "normal")                    # "normal" or "eager"
if SAFE_MODE:
    BLOCK_IMAGES = False
    PRUNE_MODE = "none"

GROUP_LOAD_WAIT = float(os.environ.get("GROUP_LOAD_WAIT", "2.5"))
SCROLL_MIN_WAIT = float(os.environ.get("SCROLL_MIN_WAIT", "0.6"))
SCROLL_MAX_WAIT = float(os.environ.get("SCROLL_MAX_WAIT", "6.0"))
STAGNANT_LIMIT = int(os.environ.get("STAGNANT_LIMIT", "4"))
OLD_STREAK_STOP = int(os.environ.get("OLD_STREAK_STOP", "8"))
TAIL_KEEP = 3                       # newest posts left alone so "See more" can expand them
PAGE_LOAD_TIMEOUT = 60
SCRIPT_TIMEOUT = 30
CMD_TIMEOUT = 60                    # selenium <-> chromedriver HTTP timeout
MAX_ATTEMPTS = 3                    # browser restarts per group
RESTART_CHROME_EVERY = int(os.environ.get("RESTART_CHROME_EVERY", "6"))  # groups per Chrome

PROGRESS_FILE = "progress.json"

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("scraper.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("fb-scraper")

_file_lock = threading.Lock()
STOP_EVENT = threading.Event()  # set on Ctrl+C so running groups stop at once


class AuthError(RuntimeError):
    pass


# ----------------------------- GROUPS
def load_groups(path):
    groups, seen = [], set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            row = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}
            url = row.get("url") or row.get("group_url") or row.get("group_ref") or ""
            match = re.search(r"facebook\.com/groups/([^/?#]+)", url, re.I)
            if not match and url.isdigit():
                match = re.match(r"(\d+)", url)
            if not match:
                log.warning("Skipping invalid group row: %s", row)
                continue
            gid = match.group(1)
            if gid in seen:
                continue
            seen.add(gid)
            groups.append({
                "group_id": gid,
                "url": f"https://www.facebook.com/groups/{gid}/",
                "group_name": row.get("name") or f"Group {gid}",
                "city": row.get("city", ""),
            })
    return groups


# ----------------------------- TIME PARSING
MONTHS = {}
for _i, _n in enumerate(("january", "february", "march", "april", "may", "june", "july",
                         "august", "september", "october", "november", "december"), 1):
    MONTHS[_n] = _i
    MONTHS[_n[:3]] = _i
MONTHS["sept"] = 9
TIME_SUFFIX = r"(?:\s+(?:at\s+)?(\d{1,2}):(\d{2})\s*(am|pm)?)?"
WEEKDAY = r"(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*"


def _hm(h, minute, ampm):
    if h is None:
        return 0, 0
    h, minute = int(h), int(minute)
    if ampm == "pm" and h < 12:
        h += 12
    if ampm == "am" and h == 12:
        h = 0
    return h, minute


def parse_fb_time(value, now):
    """Return (datetime or None, approx). approx=True for '3w' style (+/- a week)."""
    if not value:
        return None, False
    text = str(value).lower()
    text = re.sub(r"[·•\u200e\u200f\u202f\u00a0]", " ", text)
    text = re.sub(r"(?:utc|gmt)\s?[+-]\d{1,2}(?::\d{2})?", " ", text)
    text = re.sub(r"^\s*" + WEEKDAY + r",?\s+", "", text)
    text = re.sub(r"\s+", " ", text).strip(" ,.")
    text = re.sub(r"\s+ago$", "", text)
    if not text or len(text) > 60:
        return None, False
    if text in {"just now", "now"}:
        return now, False

    m = re.fullmatch(r"(\d+)\s*(m|min|mins|minutes?|h|hr|hrs|hours?|d|days?|w|wk|wks|weeks?)", text)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit.startswith("m"):
            return now - timedelta(minutes=n), False
        if unit.startswith("h"):
            return now - timedelta(hours=n), False
        if unit.startswith("d"):
            return now - timedelta(days=n), False
        return now - timedelta(weeks=n), True

    m = re.match(r"yesterday" + TIME_SUFFIX, text)
    if m:
        h, minute = _hm(*m.groups())
        return (now - timedelta(days=1)).replace(hour=h, minute=minute, second=0, microsecond=0), False

    def build(month_name, day, year, tp):
        month = MONTHS.get(month_name)
        if not month:
            return None
        h, minute = _hm(*tp)
        try:
            dt = datetime(int(year) if year else now.year, month, int(day), h, minute, tzinfo=now.tzinfo)
        except (ValueError, TypeError):
            return None
        if not year and dt > now + timedelta(days=1):
            try:
                dt = dt.replace(year=dt.year - 1)
            except ValueError:
                return None
        return dt

    m = re.match(r"([a-z]+) (\d{1,2})(?:st|nd|rd|th)?(?:,? (\d{4}))?" + TIME_SUFFIX, text)
    if m:
        return build(m.group(1), m.group(2), m.group(3), m.groups()[3:]), False
    m = re.match(r"(\d{1,2})(?:st|nd|rd|th)? ([a-z]+)(?:,? (\d{4}))?" + TIME_SUFFIX, text)
    if m:
        return build(m.group(2), m.group(1), m.group(3), m.groups()[3:]), False
    return None, False


def pb_date(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000Z")


def parse_pb_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        return None


# ----------------------------- POST IDS / LINKS
def extract_fb_id(url):
    if not url or url == "[no shared link]":
        return ""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for key in ("story_fbid", "fbid", "post_id", "multi_permalinks"):
        if query.get(key):
            m = re.search(r"\d+", query[key][0])
            if m:
                return m.group(0)
    m = re.search(r"/(?:posts|permalink|photos|videos|reel)/(\d+)", parsed.path, re.I)
    return m.group(1) if m else ""


def normalize_permalink(url):
    if not url:
        return "[no shared link]"
    url = str(url).strip()
    if url.startswith("//"):
        url = "https:" + url
    if "l.facebook.com/l.php" in url:
        q = parse_qs(urlparse(url).query)
        if q.get("u"):
            url = unquote(unquote(q["u"][0]))
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host and not (host == "facebook.com" or host.endswith(".facebook.com")):
        return "[no shared link]"
    low = url.lower()
    if any(x in low for x in ("/help", "/privacy", "/ads", "/policies", "/dialogs",
                               "/notifications", "sharer", "/login", "/watch/?v=")):
        return "[no shared link]"
    query = parse_qs(parsed.query)
    path = parsed.path.lower()
    if "permalink.php" in path and query.get("story_fbid") and query.get("id"):
        return f"https://www.facebook.com/permalink.php?story_fbid={query['story_fbid'][0]}&id={query['id'][0]}"
    if path.endswith("/photo.php") and query.get("fbid") and query.get("id"):
        return f"https://www.facebook.com/photo.php?fbid={query['fbid'][0]}&id={query['id'][0]}"
    if re.search(r"/groups/[^/?#]+/posts/[^/?#]+", parsed.path, re.I) or \
       re.search(r"/(?:posts|permalink|photos|videos|reel)/[^/?#]+", parsed.path, re.I):
        return f"https://www.facebook.com{parsed.path.rstrip('/')}/"
    if query.get("story_fbid") and query.get("id"):
        return f"https://www.facebook.com/permalink.php?story_fbid={query['story_fbid'][0]}&id={query['id'][0]}"
    return "[no shared link]"


def body_hash(group_id, body):
    norm = re.sub(r"\s+", " ", body).strip().lower()[:500]
    return "v2:" + hashlib.sha1(f"{group_id}|{norm}".encode("utf-8", "replace")).hexdigest()


# ----------------------------- POCKETBASE
class PocketBase:
    def __init__(self):
        self.api = f"{POCKETBASE_URL}/api/collections/posts/records"
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if PB_EMAIL and PB_PASSWORD:
            r = self.session.post(
                f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
                json={"identity": PB_EMAIL, "password": PB_PASSWORD}, timeout=15)
            r.raise_for_status()
            self.session.headers["Authorization"] = r.json()["token"]

    def existing(self, group_id):
        """{(field, value): posted_at_string} for every post already saved for this group."""
        known, page = {}, 1
        while True:
            try:
                r = self.session.get(self.api, params={
                    "filter": f'group_id="{group_id}"', "page": page, "perPage": 500,
                    "fields": "facebook_post_id,permalink,temporary_id,posted_at"}, timeout=30)
                if r.status_code != 200:
                    raise RuntimeError(f"Could not preload saved posts for group {group_id}: HTTP {r.status_code}")
                    return known
                payload = r.json()
                for item in payload.get("items", []):
                    pa = item.get("posted_at") or ""
                    for field in ("facebook_post_id", "permalink", "temporary_id"):
                        v = item.get(field)
                        if v:
                            known[(field, str(v))] = pa
                if page >= payload.get("totalPages", 1):
                    break
                page += 1
            except requests.RequestException as exc:
                raise RuntimeError(f"Preload failed for group {group_id}: {exc}") from exc
                break
        return known

    def create(self, data):
        r = self.session.post(self.api, json=data, timeout=20)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"PocketBase HTTP {r.status_code}: {r.text[:300]}")

    def purge_undated(self):
        deleted = 0
        while True:
            r = self.session.get(self.api, params={
                "filter": 'posted_at = "" && source = "python_scraper"', "perPage": 200, "fields": "id"}, timeout=30)
            if r.status_code != 200:
                log.error("Purge list failed: %s %s", r.status_code, r.text[:200])
                break
            items = r.json().get("items", [])
            if not items:
                break
            n = 0
            for item in items:
                d = self.session.delete(f"{self.api}/{item['id']}", timeout=15)
                if d.status_code in (200, 204):
                    n += 1
            deleted += n
            if n == 0:
                log.error("Purge could not delete records (check PocketBase delete rule)")
                break
        log.info("Purged %d undated records", deleted)


# ----------------------------- SELENIUM
COOKIE_KEYS = {"name", "value", "domain", "path", "secure", "httpOnly", "expiry", "sameSite"}
SAMESITE = {"no_restriction": "None", "none": "None", "lax": "Lax", "strict": "Strict"}




def make_driver():
    """Create a standard Chrome session with lightweight page settings."""
    o = webdriver.ChromeOptions()
    flags = ["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage",
             "--disable-software-rasterizer", "--window-size=1440,1000", "--lang=en-US"]
    for a in flags:
        o.add_argument(a)
    if HEADLESS:
        o.add_argument("--headless=new")
    o.page_load_strategy = PAGE_LOAD
    o.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2 if BLOCK_IMAGES else 1,
        "profile.default_content_setting_values.notifications": 2,
    })

    d = None
    try:
        d = webdriver.Chrome(options=o)
        d.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        d.set_script_timeout(SCRIPT_TIMEOUT)

        try:
            d.command_executor._client_config.timeout = CMD_TIMEOUT
        except Exception:
            pass

        return d

    except Exception:
        if d is not None:
            kill_driver(d)
        raise


def _find_chrome_roots(chromedriver_pid):
    """Find scraper-owned headless Chrome root processes."""
    if os.name != "nt" or not chromedriver_pid:
        return []

    ps = (
        f"$p={int(chromedriver_pid)}; "
        "Get-CimInstance Win32_Process | "
        "Where-Object { "
        "$_.Name -eq 'chrome.exe' -and "
        "$_.ParentProcessId -eq $p -and "
        "$_.CommandLine -match '--headless=new' -and "
        "$_.CommandLine -match 'scoped_dir' -and "
        "$_.CommandLine -notmatch '--type=' "
        "} | ForEach-Object { $_.ProcessId }"
    )

    try:
        result = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                ps
            ],
            capture_output=True,
            text=True,
            timeout=10
        )

        return [
            int(x.strip())
            for x in result.stdout.splitlines()
            if x.strip().isdigit()
        ]

    except Exception as exc:
        log.warning(
            "Could not identify Chrome child processes: %s", exc
        )
        return []


def kill_driver(driver):
    """Close Selenium driver and clean up its scraper-owned Chrome."""
    if driver is None:
        return

    service = getattr(driver, "service", None)
    process = getattr(service, "process", None)
    service_pid = getattr(process, "pid", None)

    # Identify Chrome roots before driver.quit() stops ChromeDriver.
    chrome_pids = _find_chrome_roots(service_pid)

    # First attempt graceful Selenium shutdown.
    t = threading.Thread(
        target=lambda: _safe(driver.quit),
        daemon=True
    )
    t.start()
    t.join(8)

    if os.name == "nt":
        # Kill only the identified scraper-owned Chrome process trees.
        for pid in chrome_pids:
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    capture_output=True,
                    timeout=15
                )
            except Exception as exc:
                log.warning(
                    "Chrome cleanup failed for PID %s: %s",
                    pid, exc
                )

        # Clean up ChromeDriver itself.
        if service_pid:
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(service_pid), "/T", "/F"],
                    capture_output=True,
                    timeout=15
                )
            except Exception as exc:
                log.warning(
                    "ChromeDriver cleanup failed: %s", exc
                )

    elif process is not None:
        try:
            process.kill()
        except Exception:
            pass


def _safe(fn):
    try:
        fn()
    except Exception:
        pass


def blocked(driver):
    url = driver.current_url.lower()
    return any(p in url for p in ("/login", "checkpoint", "two_step", "recover"))


def read_cookies():
    if not os.path.exists(COOKIE_FILE) or os.path.getsize(COOKIE_FILE) < 10:
        raise AuthError(f"{COOKIE_FILE} is missing or empty. Export your Facebook cookies again.")
    try:
        with open(COOKIE_FILE, encoding="utf-8") as f:
            cookies = json.load(f)
    except json.JSONDecodeError:
        raise AuthError(f"{COOKIE_FILE} is not valid JSON. Export your Facebook cookies again.")
    if not cookies:
        raise AuthError(f"{COOKIE_FILE} has no cookies. Export your Facebook cookies again.")
    return cookies


def login_with_cookies(driver):
    cookies = read_cookies()
    try:
        driver.get("https://www.facebook.com/")
    except TimeoutException:
        _safe(lambda: driver.execute_script("window.stop()"))
    time.sleep(1.0)
    for original in cookies:
        c = dict(original)
        if "expirationDate" in c:
            c["expiry"] = int(c.pop("expirationDate"))
        if "expires" in c and "expiry" not in c:
            try:
                c["expiry"] = int(c.pop("expires"))
            except (ValueError, TypeError):
                c.pop("expires", None)
        if "expiry" in c:
            try:
                c["expiry"] = int(c["expiry"])
            except (ValueError, TypeError):
                c.pop("expiry", None)
        ss = str(c.get("sameSite", "")).lower()
        if ss in SAMESITE:
            c["sameSite"] = SAMESITE[ss]
        else:
            c.pop("sameSite", None)
        c = {k: v for k, v in c.items() if k in COOKIE_KEYS}
        try:
            driver.add_cookie(c)
        except WebDriverException:
            pass
    try:
        driver.get("https://www.facebook.com/")
    except TimeoutException:
        _safe(lambda: driver.execute_script("window.stop()"))
    time.sleep(1.5)
    if blocked(driver):
        _safe(lambda: driver.save_screenshot("facebook_debug.png"))
        raise AuthError("Facebook login/checkpoint detected. Export facebook-cookies.json again.")


def save_cookies(driver):
    """Fetch FIRST, then write atomically, so the file can never be left empty."""
    try:
        cookies = driver.get_cookies()
        if cookies and any(c.get("name") == "c_user" for c in cookies):
            with _file_lock:
                tmp = COOKIE_FILE + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(cookies, f, ensure_ascii=False, indent=2)
                os.replace(tmp, COOKIE_FILE)
    except Exception as exc:
        log.warning("Could not save cookies (old file kept): %s", exc)


# One call: expand "See more", read finished posts (all but the newest few), tag timestamp link.
# NOTE: returns a JSON STRING (not nested objects) so chromedriver never has to serialize a big object graph.
EXTRACT_JS = r"""
const flush = arguments[0], tail = arguments[1], debug = !!arguments[2], wantImg = !!arguments[3], fastMode = !!arguments[4];
const seeRe = /^see more(…|\.\.\.)?$/i;
function visText(a) {
  // Facebook scrambles timestamp letters in the DOM and fixes the order with CSS.
  // Read what is actually visible: measure each letter, keep the main text line, sort left to right.
  const ar = a.getBoundingClientRect();
  const chars = [];
  const walker = document.createTreeWalker(a, NodeFilter.SHOW_TEXT);
  const range = document.createRange();
  let n;
  while ((n = walker.nextNode())) {
    const el = n.parentElement;
    if (!el) continue;
    let hid = false;
    for (let p = el; p; p = p.parentElement) {
      const c2 = getComputedStyle(p);
      if (c2.display === 'none' || c2.visibility === 'hidden' || parseFloat(c2.opacity) === 0 ||
          (p === el && parseFloat(c2.fontSize) === 0)) { hid = true; break; }
      if (p === a) break;
    }
    if (hid) continue;
    const txt = n.textContent;
    for (let i = 0; i < txt.length; i++) {
      const ch = txt[i];
      if (/[\s\u034f\u200b-\u200f\u2060\ufeff]/.test(ch)) continue;
      range.setStart(n, i); range.setEnd(n, i + 1);
      const r = range.getBoundingClientRect();
      if (r.width < 0.5 || r.height < 0.5) continue;
      chars.push({ch, top: Math.round(r.top), left: r.left, w: r.width});
    }
  }
  if (!chars.length) return {t: '', c: ''};
  let inside = chars.filter(c => c.left >= ar.left - 2 && c.left <= ar.right + 2 &&
                                 c.top >= ar.top - 4 && c.top <= ar.bottom + 4);
  if (!inside.length) inside = chars;
  const lines = {};
  for (const c of inside) (lines[c.top] = lines[c.top] || []).push(c);
  let best = null;
  for (const k in lines) if (!best || lines[k].length > best.length) best = lines[k];
  best.sort((x, y) => x.left - y.left);
  let out = '', prev = null;
  for (const c of best) {
    if (prev && c.left - (prev.left + prev.w) > 2.5) out += ' ';
    out += c.ch; prev = c;
  }
  return {t: out, c: chars.slice(0, 30).map(c => c.ch + '@' + c.top + ',' + Math.round(c.left)).join(' ')};
}
const roots = Array.from(document.querySelectorAll('div[role="feed"] > div'));
const limit = flush ? roots.length : Math.max(0, roots.length - tail);
window.__fbs_n = window.__fbs_n || 0;
const out = [];
for (let i = 0; i < roots.length; i++) {
  const post = roots[i];
  if (post.dataset.fbs) continue;
  for (const b of post.querySelectorAll('[role="button"]')) {
    const t = (b.innerText || '').trim();
    if (t.length < 12 && seeRe.test(t)) { try { b.click(); } catch (e) {} }
  }
  if (i >= limit) continue;

  const msg = post.querySelector('[data-ad-rendering-role="story_message"]');
  const body = ((msg && (msg.innerText || msg.textContent)) || '').trim().slice(0, 5000);
  if (!body) {
    // maybe not rendered yet: only give up after several empty passes (or at the final flush)
    const e = (parseInt(post.dataset.fbe || '0') + 1);
    post.dataset.fbe = String(e);
    if (e >= 4 || flush) post.dataset.fbs = '1';
    continue;
  }
  post.dataset.fbs = '1';
  const uid = ++window.__fbs_n;

  const prof = post.querySelector('[data-ad-rendering-role="profile_name"]');
  const author = ((prof && (prof.innerText || prof.textContent)) || '').trim() || '[no name]';
  const pa = prof && prof.querySelector('a');
  const profileLink = pa ? pa.href : '[no link]';

  let best = null, bestScore = -1, fallback = '';
  for (const a of post.querySelectorAll('a[href]')) {
    if (msg && msg.contains(a)) continue;
    if (a.closest('[aria-label^="Comment"], [aria-label^="Reply"]')) continue;
    let u; try { u = new URL(a.href, location.href); } catch (e) { continue; }
    const host = (u.hostname || '').toLowerCase();
    if (!(host === 'facebook.com' || host.endsWith('.facebook.com'))) continue;
    const p = u.pathname || '', q = u.searchParams;
    const groupPost = /\/groups\/[^/?#]+\/posts\/[^/?#]+/i.test(p);
    const pathPost = /\/(?:posts|permalink|videos|reel)\/[^/?#]+/i.test(p);
    const storyQ = q.has('story_fbid') && q.has('id');
    const photo = (/\/photo\.php$/i.test(p) && q.has('fbid')) || /\/photos\/[^/?#]+/i.test(p);
    if (!(groupPost || pathPost || storyQ || photo)) continue;
    if (a.querySelector('img')) { if (!fallback) fallback = a.href; continue; }
    let score = groupPost ? 10 : (pathPost || storyQ) ? 9 : 5;
    if ((a.innerText || '').trim().length > 0 || a.getAttribute('aria-label')) score += 1;
    if (score > bestScore) { bestScore = score; best = a; }
  }
  let permalink = fallback, ts = [];
  if (best) {
    permalink = best.href;
    ts = [best.getAttribute('aria-label') || '', (best.innerText || '').trim(), (best.textContent || '').trim()];
  }
  // Header links: Facebook often keeps the timestamp link as href="#" until hovered.
  const cands = best ? [best] : [];
  const allA = Array.from(post.querySelectorAll('a'));
  const okLink = (a) => {
    if (msg && msg.contains(a)) return false;
    if (a.querySelector('img, svg')) return false;
    if (a.closest('[aria-label^="Comment"], [aria-label^="Reply"]')) return false;
    const h = a.getAttribute('href') || '';
    if (/\/user\/|profile\.php|\/people\/|\/groups\/[^/?#]+\/?(\?|$)/i.test(h) && !/\/posts\//.test(h)) return false;
    if (prof && a === prof.querySelector('a')) return false;
    return true;
  };
  // pass 1: links before the post text (header); pass 2: the rest
  for (const pass of [0, 1]) {
    for (const a of allA) {
      if (cands.length >= (fastMode ? 2 : 5)) break;
      if (cands.includes(a) || !okLink(a)) continue;
      const after = !!(msg && (msg.compareDocumentPosition(a) & Node.DOCUMENT_POSITION_FOLLOWING));
      if ((pass === 0) === after) continue;
      cands.push(a);
    }
  }
  cands.forEach((a, k) => a.setAttribute('data-fbs-ts', uid + '-' + k));
  const chtml = cands.map(a => a.outerHTML.slice(0, 300));
  const vts = fastMode ? (cands.length ? [visText(cands[0])] : []) : cands.map(a => visText(a));
  const head = (post.innerText || '').slice(0, 500);
  let anchors = null, html = '';
  if (debug) {
    anchors = allA.slice(0, 14).map(a => ({h: (a.getAttribute('href') || '').slice(0, 90),
      t: (a.innerText || '').trim().slice(0, 30), l: (a.getAttribute('aria-label') || '').slice(0, 40)}));
    if (uid <= 3) html = post.outerHTML.slice(0, 20000);
  }
  const tEl = post.querySelector('time[datetime], abbr[data-utime]');
  if (tEl) ts.push(tEl.getAttribute('datetime') || '', tEl.getAttribute('title') || '', (tEl.innerText || '').trim());

  const images = [];
  for (const img of (wantImg ? post.querySelectorAll('img[src]') : [])) {
    if (prof && prof.contains(img)) continue;
    if (img.closest('[aria-label^="Comment"], [aria-label^="Reply"]')) continue;
    const src = img.currentSrc || img.src || '';
    if (!(src.includes('scontent') || src.includes('fbcdn'))) continue;
    const low = src.toLowerCase();
    if (['emoji', 'rsrc.php', 'reaction', 'sticker', 'blank', '.svg'].some(x => low.includes(x))) continue;
    const dims = low.match(/[sp](\d{2,4})x(\d{2,4})/);
    if (dims && (parseInt(dims[1]) < 200 || parseInt(dims[2]) < 200)) continue;
    if (!images.includes(src)) images.push(src);
  }
  out.push({uid, body, author, profileLink, permalink, ts, images, nc: cands.length, chtml, vts, head, anchors, html});
}
return JSON.stringify(out);
"""

PRUNE_JS = r"""
const mode = arguments[0];
if (mode === 'none') return;
for (const p of document.querySelectorAll('div[role="feed"] > div[data-fbs]')) {
  if (p.dataset.hid) continue;
  p.dataset.hid = '1';
  if (mode === 'empty') { const h = p.offsetHeight; p.replaceChildren(); p.style.height = h + 'px'; }
  else { const h = p.offsetHeight; p.style.height = h + 'px'; p.style.overflow = 'hidden';
         for (const c of Array.from(p.children)) c.style.display = 'none'; }
}
"""

STATE_JS = """return document.querySelectorAll(
    'div[role="feed"] > div[data-fbs]'
).length;"""
# Count actual feed nodes. Waiting on STATE_JS here was a bug: extraction only
# marks posts at the start of the next loop, so the count could not change yet.
FEED_COUNT_JS = """const f=document.querySelector('div[role="feed"]');return f?f.children.length:0;"""
TOOLTIP_JS = """const t=document.querySelector('[role="tooltip"]');return t?(t.innerText||''):'';"""
PRIVACY_JS = "return (document.body.innerText || '').slice(0, 5000)"


def detect_privacy(driver):
    try:
        m = re.search(r"\b(public|private)\s+group\b", driver.execute_script(PRIVACY_JS), re.I)
        return m.group(1).lower() if m else "unknown"
    except JavascriptException:
        return "unknown"


def hover_date(driver, uid, nc, now, state, fast=False):
    """Hover each candidate timestamp link. Returns (datetime|None, real_permalink|'').
    fast=True: only the first 2 candidates and a shorter tooltip wait."""
    dt_found, href_found = None, ""
    for k in range(min(nc, 2) if fast else nc):
        try:
            el = driver.find_element(By.CSS_SELECTOR, f'[data-fbs-ts="{uid}-{k}"]')
            ActionChains(driver).move_to_element(el).perform()
        except (NoSuchElementException, StaleElementReferenceException, MoveTargetOutOfBoundsException,
                ElementNotInteractableException, JavascriptException):
            continue
        deadline = time.time() + (0.6 if fast else 1.0)
        while time.time() < deadline:
            time.sleep(0.1 if fast else 0.15)
            try:
                txt = driver.execute_script(TOOLTIP_JS) or ""
            except JavascriptException:
                break
            if txt and txt != state["tooltip"]:
                state["tooltip"] = txt
                d, _ = parse_fb_time(txt, now)
                if d:
                    dt_found = d
                    break
        try:
            np_ = normalize_permalink(el.get_attribute("href") or "")
            if np_ != "[no shared link]":
                href_found = np_
        except (StaleElementReferenceException, JavascriptException):
            pass
        if dt_found:
            break
    return dt_found, href_found


def scroll_and_wait(driver, st):
    """Scroll and wait for Facebook to append actual feed nodes."""
    poll = 0.15 if SCROLL_MIN_WAIT <= 0.3 else 0.25

    for attempt in range(2):
        baseline = driver.execute_script(FEED_COUNT_JS) or 0
        st["maxcount"] = baseline

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(SCROLL_MIN_WAIT)
        deadline = time.monotonic() + SCROLL_MAX_WAIT

        while time.monotonic() < deadline:
            count = driver.execute_script(FEED_COUNT_JS) or 0
            if count > baseline:
                st["maxcount"] = count
                return True
            time.sleep(poll)

        if attempt == 0:
            # A small reverse/forward movement can trigger lazy loading without
            # waiting for the full timeout a second time.
            driver.execute_script("window.scrollBy(0, -600)")
            time.sleep(0.15)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")

    return False


# ----------------------------- SCRAPE ONE GROUP
def scrape_group(driver, group, pb, cutoff, until, max_scrolls, args):
    gid = group["group_id"]
    stats = {"found": 0, "new": 0, "dups": 0, "errors": 0, "old": 0, "unknown_date": 0}
    url = group["url"] + "?sorting_setting=CHRONOLOGICAL"
    log.info("[%s] Opening %s", gid, url)
    try:
        driver.get(url)
    except TimeoutException:
        _safe(lambda: driver.execute_script("window.stop()"))
    time.sleep(GROUP_LOAD_WAIT)
    if blocked(driver):
        _safe(lambda: driver.save_screenshot("facebook_debug.png"))
        raise AuthError("Facebook login/checkpoint while opening a group")

    privacy = detect_privacy(driver)
    if privacy == "private" and not args.include_private:
        log.info("[%s] private group, skipped", gid)
        return stats, "private"

    tz = datetime.now().astimezone().tzinfo
    now = datetime.now(tz)
    cutoff_l = cutoff.astimezone(tz)
    until_l = until.astimezone(tz) if until else None

    known = pb.existing(gid)
    log.info("[%s] %d keys already in PocketBase", gid, len(known))
    seen = set()
    st = {"old_streak": 0, "last_dt": None, "tooltip": "", "dbg": 0, "dump": 0, "known_streak": 0, "maxcount": None, "maxheight": 0}
    t0 = time.time()

    def near_boundary(dt):
        span = timedelta(days=8)
        return abs(dt - cutoff_l) <= span or (until_l is not None and abs(dt - until_l) <= span)

    def process(flush):
        res = driver.execute_script(EXTRACT_JS, flush, TAIL_KEEP, bool(args.debug_dates), not args.no_images, bool(args.fast))
        try:
            raws = json.loads(res) if res else []
        except (ValueError, TypeError):
            log.warning("[%s] extract returned unreadable data, skipping this pass", gid)
            raws = []
        for raw in raws:
            body = (raw.get("body") or "").strip()[:5000]
            if not body:
                continue
            bk = hashlib.md5(body.encode("utf-8", "replace")).hexdigest()
            if bk in seen:
                continue
            seen.add(bk)

            permalink = normalize_permalink(raw.get("permalink", ""))
            author = (raw.get("author") or "[no name]").strip()
            fb_id = extract_fb_id(permalink)
            temp_id = body_hash(gid, body)
            keys = []
            if fb_id:
                keys.append(("facebook_post_id", fb_id))
            if permalink != "[no shared link]":
                keys.append(("permalink", permalink))
            keys.append(("temporary_id", temp_id))

            # ---- already saved: reuse saved date, no hover, no DB write
            hit = next((k for k in keys if k in known), None)
            if hit:
                stats["dups"] += 1
                st["known_streak"] += 1
                d = parse_pb_date(known[hit])
                if d:
                    d = d.astimezone(tz)
                    st["last_dt"] = d
                    if d < cutoff_l:
                        st["old_streak"] += 1
                    else:
                        st["old_streak"] = 0
                continue

            # ---- new post: find its date
            st["known_streak"] = 0
            dt, approx, source = None, False, ""
            for cand in list(raw.get("ts") or []) + [(v or {}).get("t", "") for v in (raw.get("vts") or [])]:
                d, ap = parse_fb_time(cand, now)
                if d:
                    dt, approx, source = d, ap, "text"
                    break
            if dt is None:
                for line in re.split(r"[\n·•|]", raw.get("head") or "")[:8]:
                    line = line.strip()
                    if 0 < len(line) <= 40:
                        d, ap = parse_fb_time(line, now)
                        if d:
                            dt, approx, source = d, ap, "head"
                            break
            if dt is None or (approx and (args.exact_dates or near_boundary(dt))):
                d, href = hover_date(driver, raw.get("uid"), int(raw.get("nc") or 0), now, st,
                                     fast=args.fast)
                if d:
                    dt, approx, source = d, False, "hover"
                if href:
                    permalink = href
                    fb_id = extract_fb_id(permalink)
                    keys = [("facebook_post_id", fb_id)] if fb_id else []
                    keys += [("permalink", permalink), ("temporary_id", temp_id)]
            if dt is not None and approx:
                source = "approx"
            if args.debug_dates and st["dbg"] < 25:
                st["dbg"] += 1
                log.info("[%s] DATE DEBUG -> %s (%s) | VT=%s", gid, dt, source or "none",
                         [(v or {}).get("t") for v in (raw.get("vts") or [])])
                if source in ("", "none") and raw.get("vts"):
                    log.info("[%s]   CHARS=%s", gid, (raw["vts"][0] or {}).get("c"))
                if raw.get("html") and st["dump"] < 2:
                    st["dump"] += 1
                    with open(f"debug_post_{st['dump']}.html", "w", encoding="utf-8") as fh:
                        fh.write(raw["html"])
            if dt is None and st["last_dt"] is not None:
                dt, source = st["last_dt"], "inferred"   # feed is chronological: same time as previous post
            if dt is not None and source != "inferred":
                st["last_dt"] = dt

            if dt and until_l and dt >= until_l:
                continue
            if dt and dt < cutoff_l:
                stats["old"] += 1
                if source != "inferred":
                    st["old_streak"] += 1
                continue
            if dt and source != "inferred":
                st["old_streak"] = 0
            if dt is None:
                stats["unknown_date"] += 1
                if stats["unknown_date"] == 10 and stats["found"] == 10:
                    log.warning("[%s] first 10 posts have NO date. Run with --debug-dates and send me the log.", gid)

            stats["found"] += 1
            data = {
                "group_id": gid, "group_name": group["group_name"], "group_url": group["url"],
                "city": group.get("city", ""), "body": body, "author": author,
                "author_profile_url": raw.get("profileLink") or "[no link]",
                "image_urls": raw.get("images") or [], "images": raw.get("images") or [],
                "image_count": len(raw.get("images") or []),
                "post_type": "facebook_post", "permalink": permalink, "post_url": permalink,
                "facebook_post_id": fb_id, "temporary_id": temp_id,
                "scraped_at": pb_date(datetime.now(timezone.utc)),
                "source": "python_scraper", "date_source": source or "none",
            }
            if dt:
                data["posted_at"] = pb_date(dt)
            try:
                pb.create(data)
                stats["new"] += 1
                for k in keys:
                    known[k] = data.get("posted_at", "")
            except Exception as exc:
                stats["errors"] += 1
                log.error("[%s] PocketBase create failed: %s", gid, exc)
        return st["old_streak"] >= OLD_STREAK_STOP or (
            args.catchup > 0 and st["known_streak"] >= args.catchup)

    def safe_process(flush):
        """Retry once in place on a transient extract failure, instead of restarting Chrome."""
        try:
            return process(flush)
        except WebDriverException as exc:
            msg = str(exc)
            if "deserialize" not in msg and "Runtime.callFunctionOn" not in msg:
                raise
            log.warning("[%s] extract failed once (%.80s), retrying", gid, msg.replace("\n", " "))
            time.sleep(2)
            return process(flush)

    stop_reason, stagnant = "max_scrolls", 0
    for i in range(1, max_scrolls + 1):
        if STOP_EVENT.is_set():
            stop_reason = "interrupted"
            break
        if safe_process(False):
            stop_reason = ("caught_up" if (args.catchup > 0 and st["known_streak"] >= args.catchup)
                           else "cutoff_reached")
            break
        driver.execute_script(PRUNE_JS, PRUNE_MODE)
        if scroll_and_wait(driver, st):
            stagnant = 0
        else:
            stagnant += 1
            if args.debug_dates:
                log.info("[%s] no growth (stagnant %d/%d): feed children=%s height=%s", gid, stagnant,
                         STAGNANT_LIMIT, st["maxcount"], st["maxheight"])
            if stagnant >= STAGNANT_LIMIT:
                safe_process(True)
                driver.execute_script(PRUNE_JS, PRUNE_MODE)
                stop_reason = "feed_exhausted"
                break
        if i % 10 == 0:
            ld = st["last_dt"].strftime("%b %d") if st["last_dt"] else "?"
            el = max(1.0, time.time() - t0)
            seen_n = stats["new"] + stats["dups"] + stats["old"]
            log.info("[%s] scroll %d/%d | at=%s new=%d dups=%d old=%d undated=%d | %.0fs | %.0f posts/min",
                     gid, i, max_scrolls, ld, stats["new"], stats["dups"], stats["old"],
                     stats["unknown_date"], el, seen_n / el * 60)

    if stop_reason == "feed_exhausted" and st["last_dt"] and st["last_dt"] > cutoff_l + timedelta(days=3):
        stop_reason = "stalled"   # feed stopped loading before reaching the start date -> retry next run
    log.info("[%s] finished: %s found=%d new=%d dups=%d old=%d undated=%d errors=%d",
             gid, stop_reason, stats["found"], stats["new"], stats["dups"], stats["old"],
             stats["unknown_date"], stats["errors"])
    return stats, stop_reason


# ----------------------------- PROGRESS / WORKERS
def read_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


def write_json_atomic(path, data):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def load_progress(path, range_key):
    data = read_json(path, {})
    if data.get("range") != range_key:
        data = {"range": range_key}
    for k in ("done", "partial", "empty", "private"):
        data.setdefault(k, [])
    return data


class Ctx:
    def __init__(self, args, cutoff, until, max_scrolls, progress):
        self.args, self.cutoff, self.until, self.max_scrolls = args, cutoff, until, max_scrolls
        self.progress = progress
        self.lock = threading.Lock()
        self.stop = STOP_EVENT
        self.totals = {"found": 0, "new": 0, "dups": 0, "errors": 0, "old": 0, "unknown_date": 0}

    def record(self, gid, stats, reason):
        with self.lock:
            p = self.progress
            for k in self.totals:
                self.totals[k] += stats.get(k, 0)
            sets = {k: set(p[k]) for k in ("done", "partial", "empty", "private")}
            if reason == "private":
                sets["private"].add(gid); sets["done"].add(gid); sets["partial"].discard(gid)
            elif reason in ("cutoff_reached", "feed_exhausted", "caught_up"):
                sets["done"].add(gid); sets["partial"].discard(gid)
                if stats["found"] == 0 and stats["new"] == 0:
                    sets["empty"].add(gid)
            else:  # max_scrolls / stalled / failed -> retry next run (resume is cheap)
                sets["partial"].add(gid); sets["done"].discard(gid)
            for k, v in sets.items():
                p[k] = sorted(v)
            write_json_atomic(self.args.progress, p)


def worker(wid, q, ctx):
    time.sleep(wid * 6)  # stagger logins
    try:
        pb = PocketBase()
    except Exception as exc:
        log.error("[w%d] PocketBase connection failed: %s", wid, exc)
        return
    driver, served = None, 0
    try:
        while not ctx.stop.is_set():
            try:
                group = q.get_nowait()
            except queue.Empty:
                break
            gid, ok = group["group_id"], False
            for attempt in range(1, MAX_ATTEMPTS + 1):
                if ctx.stop.is_set():
                    break
                try:
                    if driver is None:
                        driver = make_driver()
                        login_with_cookies(driver)
                    stats, reason = scrape_group(driver, group, pb, ctx.cutoff, ctx.until,
                                                 ctx.max_scrolls, ctx.args)
                    ctx.record(gid, stats, reason)
                    ok = True
                    break
                except AuthError as exc:
                    log.error("[w%d] %s -- stopping run", wid, exc)
                    ctx.stop.set()
                    break
                except Exception as exc:
                    log.warning("[w%d] group %s attempt %d/%d failed (%s: %.120s); restarting Chrome",
                                wid, gid, attempt, MAX_ATTEMPTS, type(exc).__name__, exc)
                    kill_driver(driver)
                    driver = None
                    time.sleep(3)
            if not ok and not ctx.stop.is_set():
                ctx.record(gid, {}, "failed")
            served += 1
            if driver is not None and served % RESTART_CHROME_EVERY == 0:
                save_cookies(driver)
                kill_driver(driver)
                driver = None
            time.sleep(random.uniform(0.5, 1.5))
    finally:
        if driver is not None:
            if not ctx.stop.is_set():
                save_cookies(driver)
            kill_driver(driver)


# ----------------------------- EXPORT
def export_csv(pb, since, until, path):
    start, end = pb_date(since), pb_date(until)
    flt = f'(posted_at >= "{start}" && posted_at < "{end}")'
    rows, page = [], 1
    while True:
        r = pb.session.get(pb.api, params={"filter": flt, "perPage": 200, "page": page, "sort": "posted_at"},
                           timeout=30)
        if r.status_code != 200:
            log.error("Export failed: HTTP %s %s", r.status_code, r.text[:300])
            return
        payload = r.json()
        rows.extend(payload.get("items", []))
        if page >= payload.get("totalPages", 1):
            break
        page += 1
    cols = ["group_id", "group_name", "city", "author", "author_profile_url", "posted_at",
            "permalink", "body", "image_urls"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for item in rows:
            w.writerow([json.dumps(item.get(c), ensure_ascii=False) if c == "image_urls" else item.get(c, "")
                        for c in cols])
    log.info("Exported %d records to %s", len(rows), path)


# ----------------------------- MAIN
def run(args):
    all_groups = load_groups(args.csv)
    # User-facing group positions are 1-based; --end is inclusive.
    groups = all_groups[args.start - 1:args.end]
    if not groups:
        log.error("No groups loaded. Check CSV path/header and --start/--end.")
        return
    try:
        read_cookies()
    except AuthError as exc:
        log.error("%s", exc)
        return
    if os.path.getsize(COOKIE_FILE) > 10:
        shutil.copyfile(COOKIE_FILE, COOKIE_FILE + ".bak")

    tz = datetime.now().astimezone().tzinfo
    now_local = datetime.now(tz)
    if args.since:
        cutoff = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=tz)
        until = datetime.strptime(args.until, "%Y-%m-%d").replace(tzinfo=tz) if args.until else None
    else:
        cutoff, until = now_local - timedelta(days=args.days), None

    range_key = f"{args.since or cutoff.date()}_{args.until or 'now'}"
    progress = load_progress(args.progress, range_key)
    done = set(progress["done"])
    todo = [g for g in groups if g["group_id"] not in done]
    max_scrolls = args.max_scrolls or 1500

    log.info("=" * 60)
    log.info("Range %s -> %s | groups=%d todo=%d | workers=%d | max_scrolls=%d",
             cutoff.date(), until.date() if until else "now", len(groups), len(todo), args.workers, max_scrolls)
    log.info("=" * 60)

    pb = PocketBase()
    if args.purge_undated:
        pb.purge_undated()

    ctx = Ctx(args, cutoff, until, max_scrolls, progress)
    q = queue.Queue()
    for g in todo:
        q.put(g)
    threads = [threading.Thread(target=worker, args=(i, q, ctx), daemon=True)
               for i in range(max(1, min(args.workers, len(todo) or 1)))]
    
    started_threads = []

    try:
        for t in threads:
            t.start()
            started_threads.append(t)

        while any(t.is_alive() for t in started_threads):
            time.sleep(1)

    except KeyboardInterrupt:
        log.warning(
            "Ctrl+C received: requesting graceful worker shutdown..."
        )
        ctx.stop.set()

        # Wait for all started workers to finish their cleanup.
        log.info("Waiting for workers to close their Chrome drivers...")

        try:
            for t in started_threads:
                while t.is_alive():
                    t.join(timeout=2)

        except KeyboardInterrupt:
            log.warning(
                "Another Ctrl+C received. Continuing to wait for safe cleanup."
            )

            # Ignore further Ctrl+C while workers finish cleanup.
            import signal

            old_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)

            try:
                for t in started_threads:
                    while t.is_alive():
                        t.join(timeout=2)
            finally:
                signal.signal(signal.SIGINT, old_sigint)

        log.info("All worker threads have stopped.")


    log.info("RUN TOTALS: %s", ctx.totals)
    log.info("done=%d partial=%d private=%d (progress file: %s)", len(progress["done"]),
             len(progress["partial"]), len(progress["private"]), args.progress)
    if progress["partial"]:
        log.info("Not finished (run the same command again to resume): %s", progress["partial"])
    if args.export:
        export_csv(pb, cutoff, until or (datetime.now(tz) + timedelta(days=1)), args.export)


def main():
    global PRUNE_MODE, SCROLL_MIN_WAIT, SCROLL_MAX_WAIT, GROUP_LOAD_WAIT
    ap = argparse.ArgumentParser(description="Authorized Facebook public-group collection -> PocketBase")
    ap.add_argument("--october", action="store_true", help="2026-10-01 inclusive -> 2026-11-01 exclusive")
    ap.add_argument("--csv", default=GROUPS_CSV)
    ap.add_argument("--since", help="inclusive start YYYY-MM-DD")
    ap.add_argument("--until", help="exclusive end YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--start", type=int, default=1, help="first group position in CSV (1-based)")
    ap.add_argument("--end", type=int, default=None, help="last group position in CSV (1-based, inclusive)")
    ap.add_argument("--workers", type=int, default=2, help="parallel Chrome windows (default: 2 to reduce RAM use)")
    ap.add_argument("--max-scrolls", type=int, default=None)
    ap.add_argument("--progress", default=PROGRESS_FILE)
    ap.add_argument("--reset-progress", action="store_true")
    ap.add_argument("--include-private", action="store_true")
    ap.add_argument("--no-images", action="store_true", help="do not save image URLs (on by default)")
    ap.add_argument("--exact-dates", action="store_true", help="hover to get exact dates for '3w' style times (slower)")
    ap.add_argument("--debug-dates", action="store_true", help="log raw timestamp text for the first posts of each group")
    ap.add_argument("--purge-undated", action="store_true", help="delete old scraper records that have no posted_at, first")
    ap.add_argument("--export", help="export in-range records to CSV")
    ap.add_argument("--catchup", type=int, default=0,
                    help="stop a group after N posts in a row that are already saved (0 = off). Refresh runs only")
    ap.add_argument("--fast", action="store_true",
                    help="faster: cheaper date hovering, shorter scroll wait, frees page memory (PRUNE_MODE=empty)")
    # accepted for backward compatibility
    ap.add_argument("--batch-size", type=int, default=None)
    ap.add_argument("--batch-pause", type=float, default=0)
    ap.add_argument("--no-cleanup", action="store_true")
    ap.add_argument("--keep-days", type=int, default=30)
    args = ap.parse_args()

    if args.start < 1:
        ap.error("--start must be 1 or greater (group positions are 1-based)")
    if args.end is not None and args.end < args.start:
        ap.error("--end must be greater than or equal to --start")
    if args.workers < 1:
        ap.error("--workers must be 1 or greater")

    if args.fast:
        if PRUNE_MODE == "hide":
            PRUNE_MODE = "empty"
        SCROLL_MIN_WAIT = min(SCROLL_MIN_WAIT, 0.3)
        SCROLL_MAX_WAIT = min(SCROLL_MAX_WAIT, 2.5)
        GROUP_LOAD_WAIT = min(GROUP_LOAD_WAIT, 1.5)
    if args.october:
        args.since, args.until = "2026-10-01", "2026-11-01"
    if args.until and not args.since:
        ap.error("--until requires --since")
    if args.reset_progress and os.path.exists(args.progress):
        os.remove(args.progress)
        log.warning("Progress reset.")
    run(args)


if __name__ == "__main__":
    main()

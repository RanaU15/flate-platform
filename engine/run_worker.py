#!/usr/bin/env python3
"""
Worker adapter that executes the existing Python Facebook scraping logic for a single target run.
Connects ONLY to PocketBase on port 8092.
Accepts:
  --group-url: Target Facebook Group URL
  --post-limit: 10, 20, 50, or 100
  --run-id: ID of the scraping_runs record
"""

import argparse
import collections
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
import requests

# Exclusively use Port 8092
os.environ["POCKETBASE_URL"] = os.environ.get("POCKETBASE_URL", "http://127.0.0.1:8092")
os.environ["PB_EMAIL"] = os.environ.get("PB_EMAIL", "admin@test.com")
os.environ["PB_PASSWORD"] = os.environ.get("PB_PASSWORD", "admin@123")
os.environ["COOKIE_FILE"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "facebook-cookies.json")

import fb_scraper

# Ensure unbuffered streams
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("worker-runner")

PB_URL = "http://127.0.0.1:8092"
EMAIL = os.environ["PB_EMAIL"]
PASSWORD = os.environ["PB_PASSWORD"]

class RunManager:
    def __init__(self, run_id):
        self.run_id = run_id
        self.session = requests.Session()
        r = self.session.post(f"{PB_URL}/api/collections/_superusers/auth-with-password",
                              json={"identity": EMAIL, "password": PASSWORD}, timeout=15)
        r.raise_for_status()
        self.token = r.json()["token"]
        self.session.headers["Authorization"] = self.token

    def log(self, level, message):
        timestamp = datetime.now(timezone.utc).isoformat()
        print(f"[{level}] {message}", flush=True)
        try:
            self.session.post(f"{PB_URL}/api/collections/scraping_run_logs/records", json={
                "run_id": self.run_id,
                "level": level,
                "message": message,
                "timestamp": timestamp
            }, timeout=5)
        except Exception as e:
            print(f"Error logging to PB: {e}", file=sys.stderr)

    def update_run(self, **kwargs):
        try:
            self.session.patch(f"{PB_URL}/api/collections/scraping_runs/records/{self.run_id}", json=kwargs, timeout=10)
        except Exception as e:
            print(f"Error updating run {self.run_id}: {e}", file=sys.stderr)

def extract_group_info(url):
    match = re.search(r"facebook\.com/groups/([^/?#]+)", url, re.I)
    if match:
        gid = match.group(1)
        return gid, f"https://www.facebook.com/groups/{gid}/"
    return "UNKNOWN", url

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group-url", required=True)
    parser.add_argument("--post-limit", type=int, default=10)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    run_mgr = RunManager(args.run_id)
    gid, canonical_url = extract_group_info(args.group_url)
    start_time = datetime.now(timezone.utc)

    run_mgr.update_run(
        status="running",
        group_id=gid,
        group_url=canonical_url,
        start_time=start_time.isoformat(),
        pid=os.getpid(),
        scraped_count=0
    )
    run_mgr.log("INFO", f"Starting Facebook scraping run {args.run_id} for group {gid} (Limit: {args.post_limit})")

    # Connect to isolated PocketBase for post storage
    pb = fb_scraper.PocketBase()

    driver = None
    try:
        run_mgr.log("INFO", "Initializing Chrome browser instance...")
        driver = fb_scraper.make_driver()
        run_mgr.log("INFO", "Logging into Facebook using cookies...")
        fb_scraper.login_with_cookies(driver)
        run_mgr.log("INFO", "Facebook authentication ready. Navigating to group feed...")

        target_feed = f"{canonical_url.rstrip('/')}/?sorting_setting=CHRONOLOGICAL"
        driver.get(target_feed)
        time.sleep(3.5)

        title = driver.title or ""
        run_mgr.log("INFO", f"Group page loaded: {title}")
        run_mgr.update_run(group_name=title.replace(" | Facebook", "").strip())

        known = pb.existing(gid)
        run_mgr.log("INFO", f"Preloaded {len(known)} existing records for deduplication.")

        posts_saved = 0
        scroll_attempt = 0
        seen_bodies = set()
        stagnant_count = 0
        scroll_state = {"maxcount": None, "maxheight": 0}

        while posts_saved < args.post_limit:
            scroll_attempt += 1

            # Extract posts using the engine's EXTRACT_JS (flush=False leaves tail for rendering)
            res = driver.execute_script(fb_scraper.EXTRACT_JS, False, fb_scraper.TAIL_KEEP, False, True, True)
            try:
                raws = json.loads(res) if res else []
            except Exception:
                raws = []

            new_in_pass = 0
            now_dt = datetime.now(timezone.utc)

            for raw in raws:
                if posts_saved >= args.post_limit:
                    break
                body = (raw.get("body") or "").strip()[:5000]
                if not body:
                    continue
                body_hash = fb_scraper.hashlib.md5(body.encode("utf-8", "replace")).hexdigest()
                if body_hash in seen_bodies:
                    continue
                seen_bodies.add(body_hash)

                permalink = fb_scraper.normalize_permalink(raw.get("permalink", ""))
                author = (raw.get("author") or "Facebook User").strip()
                fb_id = fb_scraper.extract_fb_id(permalink)
                temp_id = fb_scraper.body_hash(gid, body)

                keys = []
                if fb_id:
                    keys.append(("facebook_post_id", fb_id))
                if permalink != "[no shared link]":
                    keys.append(("permalink", permalink))
                keys.append(("temporary_id", temp_id))

                hit = next((k for k in keys if k in known), None)
                if hit:
                    continue

                # Parse post date
                dt = None
                for cand in list(raw.get("ts") or []) + [(v or {}).get("t", "") for v in (raw.get("vts") or [])]:
                    parsed, _ = fb_scraper.parse_fb_time(cand, now_dt)
                    if parsed:
                        dt = parsed
                        break

                images = raw.get("images") or []
                post_record = {
                    "run_id": args.run_id,
                    "group_id": gid,
                    "group_name": title,
                    "group_url": canonical_url,
                    "body": body,
                    "author": author,
                    "author_profile_url": raw.get("author_profile_url", ""),
                    "post_type": "facebook_post",
                    "permalink": permalink,
                    "post_url": permalink,
                    "facebook_post_id": fb_id or "",
                    "temporary_id": temp_id,
                    "image_urls": images,
                    "images": images,
                    "image_count": len(images),
                    "source": "fb_platform_runner",
                    "scraped_at": fb_scraper.pb_date(datetime.now(timezone.utc))
                }
                if dt:
                    post_record["posted_at"] = fb_scraper.pb_date(dt)

                try:
                    pb.create(post_record)
                    posts_saved += 1
                    new_in_pass += 1
                    for k in keys:
                        known[k] = post_record.get("posted_at", "")
                    run_mgr.log("INFO", f"Saved post {posts_saved}/{args.post_limit} | Author: {author} | Post ID: {fb_id or 'temp'}")
                    run_mgr.update_run(scraped_count=posts_saved)
                except Exception as ex:
                    run_mgr.log("WARN", f"Could not save post: {ex}")

            if posts_saved >= args.post_limit:
                run_mgr.log("INFO", f"Requested limit of {args.post_limit} posts reached!")
                break

            # Execute prune to free DOM nodes and trigger lazy load as in original script
            driver.execute_script(fb_scraper.PRUNE_JS, "hide")

            if new_in_pass == 0:
                stagnant_count += 1
                if stagnant_count >= 15:
                    # Final flush before giving up
                    flush_res = driver.execute_script(fb_scraper.EXTRACT_JS, True, 0, False, True, True)
                    try:
                        flush_raws = json.loads(flush_res) if flush_res else []
                    except Exception:
                        flush_raws = []
                    for raw in flush_raws:
                        if posts_saved >= args.post_limit:
                            break
                        body = (raw.get("body") or "").strip()[:5000]
                        if not body:
                            continue
                        body_hash = fb_scraper.hashlib.md5(body.encode("utf-8", "replace")).hexdigest()
                        if body_hash in seen_bodies:
                            continue
                        seen_bodies.add(body_hash)
                        permalink = fb_scraper.normalize_permalink(raw.get("permalink", ""))
                        author = (raw.get("author") or "Facebook User").strip()
                        fb_id = fb_scraper.extract_fb_id(permalink)
                        temp_id = fb_scraper.body_hash(gid, body)
                        keys = [("temporary_id", temp_id)]
                        if fb_id:
                            keys.append(("facebook_post_id", fb_id))
                        if next((k for k in keys if k in known), None):
                            continue
                        post_record = {
                            "run_id": args.run_id,
                            "group_id": gid,
                            "group_name": title,
                            "group_url": canonical_url,
                            "body": body,
                            "author": author,
                            "post_type": "facebook_post",
                            "permalink": permalink,
                            "post_url": permalink,
                            "facebook_post_id": fb_id or "",
                            "temporary_id": temp_id,
                            "image_urls": raw.get("images") or [],
                            "images": raw.get("images") or [],
                            "image_count": len(raw.get("images") or []),
                            "source": "fb_platform_runner",
                            "scraped_at": fb_scraper.pb_date(datetime.now(timezone.utc))
                        }
                        try:
                            pb.create(post_record)
                            posts_saved += 1
                            run_mgr.log("INFO", f"Saved post {posts_saved}/{args.post_limit} | Author: {author} | Post ID: {fb_id or 'temp'}")
                            run_mgr.update_run(scraped_count=posts_saved)
                        except Exception:
                            pass
                    run_mgr.log("WARN", "Feed reached end or no more new posts loading.")
                    break
            else:
                stagnant_count = 0

            # Use proven scroll_and_wait from fb_scraper
            if not fb_scraper.scroll_and_wait(driver, scroll_state):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.0)

        end_time = datetime.now(timezone.utc)
        dur = str(end_time - start_time).split(".")[0]
        run_mgr.update_run(
            status="completed",
            scraped_count=posts_saved,
            end_time=end_time.isoformat(),
            duration=dur
        )
        run_mgr.log("INFO", f"Run {args.run_id} completed successfully! Extracted {posts_saved} posts in {dur}.")

    except Exception as e:
        end_time = datetime.now(timezone.utc)
        dur = str(end_time - start_time).split(".")[0]
        run_mgr.log("ERROR", f"Scraper error: {e}")
        run_mgr.update_run(
            status="failed",
            error=str(e),
            end_time=end_time.isoformat(),
            duration=dur
        )
        sys.exit(1)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

if __name__ == "__main__":
    main()

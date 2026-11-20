import json
import time
import fb_scraper

d = fb_scraper.make_driver()
try:
    fb_scraper.login_with_cookies(d)
    url = "https://www.facebook.com/groups/flatsharemumbai/?sorting_setting=CHRONOLOGICAL"
    d.get(url)
    time.sleep(5)
    
    st = {"maxcount": None, "maxheight": 0}
    seen = set()
    total_found = 0
    
    for scroll in range(1, 10):
        # In original fb_scraper, safe_process(False) calls EXTRACT_JS with flush=False, TAIL_KEEP=3
        res = d.execute_script(fb_scraper.EXTRACT_JS, False, fb_scraper.TAIL_KEEP, False, True, True)
        try:
            raws = json.loads(res) if res else []
        except Exception:
            raws = []
        
        new_this_step = 0
        for r in raws:
            body = (r.get("body") or "").strip()[:60]
            if not body or body in seen:
                continue
            seen.add(body)
            new_this_step += 1
            total_found += 1
            print(f"[{scroll}] + Post {total_found}: {r.get('author')} | {body}")
            
        d.execute_script(fb_scraper.PRUNE_JS, "hide")
        appended = fb_scraper.scroll_and_wait(d, st)
        print(f"Scroll {scroll}: new={new_this_step}, total={total_found}, feed_appended={appended}")
finally:
    d.quit()


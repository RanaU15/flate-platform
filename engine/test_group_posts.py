import json
import time
import fb_scraper

d = fb_scraper.make_driver()
try:
    fb_scraper.login_with_cookies(d)
    url = "https://www.facebook.com/groups/flatsharemumbai/?sorting_setting=CHRONOLOGICAL"
    d.get(url)
    time.sleep(6)
    print("CURRENT_URL:", d.current_url)
    print("TITLE:", d.title)
    
    pb = fb_scraper.PocketBase()
    known = pb.existing("flatsharemumbai")
    print(f"EXISTING KNOWN KEYS FOR flatsharemumbai: {len(known)}")
    
    raw_json = d.execute_script(fb_scraper.EXTRACT_JS, True, 0, False, True, True)
    raws = json.loads(raw_json) if raw_json else []
    print(f"RAW POSTS EXTRACTED BY EXTRACT_JS: {len(raws)}")
    
    for i, r in enumerate(raws, 1):
        body = (r.get("body") or "").strip()[:60]
        permalink = fb_scraper.normalize_permalink(r.get("permalink", ""))
        fb_id = fb_scraper.extract_fb_id(permalink)
        temp_id = fb_scraper.body_hash("flatsharemumbai", r.get("body") or "")
        
        is_known = False
        for k in [("facebook_post_id", fb_id), ("permalink", permalink), ("temporary_id", temp_id)]:
            if k in known:
                is_known = True
                break
        print(f"Post {i}: {r.get('author')} | fb_id={fb_id} | ALREADY IN DB={is_known} | Body: {body}")
finally:
    d.quit()


import json
import time
from selenium import webdriver
import fb_scraper

driver = fb_scraper.make_driver()
try:
    fb_scraper.login_with_cookies(driver)
    driver.get("https://www.facebook.com/groups/flatsharemumbai/?sorting_setting=CHRONOLOGICAL")
    time.sleep(5)
    print("PAGE TITLE:", driver.title)
    roots = driver.execute_script("return document.querySelectorAll('div[role=\"feed\"] > div').length;")
    print("INITIAL FEED CHILDREN:", roots)

    res = driver.execute_script(fb_scraper.EXTRACT_JS, True, 0, False, True, True)
    posts = json.loads(res) if res else []
    print(f"EXTRACT PASS 1 FOUND: {len(posts)} posts")
    for idx, p in enumerate(posts[:5], 1):
        print(f"  {idx}. {p.get('author')}: {p.get('body', '')[:60]}...")

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)
    roots2 = driver.execute_script("return document.querySelectorAll('div[role=\"feed\"] > div').length;")
    print("AFTER SCROLL FEED CHILDREN:", roots2)

    res2 = driver.execute_script(fb_scraper.EXTRACT_JS, True, 0, False, True, True)
    posts2 = json.loads(res2) if res2 else []
    print(f"EXTRACT PASS 2 FOUND: {len(posts2)} posts")
    for idx, p in enumerate(posts2[:5], 1):
        print(f"  {idx}. {p.get('author')}: {p.get('body', '')[:60]}...")
finally:
    driver.quit()


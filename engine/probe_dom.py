import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import fb_scraper

driver = fb_scraper.make_driver()
try:
    fb_scraper.login_with_cookies(driver)
    driver.get("https://www.facebook.com/groups/flatsharemumbai/")
    time.sleep(5)
    print("PAGE TITLE:", driver.title)
    
    # Check what scroll container or elements exist
    res = driver.execute_script("""
        const res = {};
        res.bodyHeight = document.body.scrollHeight;
        res.docHeight = document.documentElement.scrollHeight;
        res.windowHeight = window.innerHeight;
        res.feed = !!document.querySelector('div[role="feed"]');
        res.feedChildren = document.querySelectorAll('div[role="feed"] > div').length;
        res.articles = document.querySelectorAll('[role="article"]').length;
        return res;
    """)
    print("DOM CHECK:", res)
finally:
    driver.quit()


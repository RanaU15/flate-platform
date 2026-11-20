import json
import time
from selenium import webdriver
import fb_scraper

driver = fb_scraper.make_driver()
try:
    fb_scraper.login_with_cookies(driver)
    driver.get("https://www.facebook.com/groups/flatsharemumbai/?sorting_setting=CHRONOLOGICAL")
    time.sleep(5)
    
    # Facebook modern feeds often scroll on document.documentElement or window
    for s in range(5):
        h_before = driver.execute_script("return document.body.scrollHeight;")
        driver.execute_script("window.scrollBy(0, 1800);")
        time.sleep(2)
        h_after = driver.execute_script("return document.body.scrollHeight;")
        articles = driver.execute_script("return document.querySelectorAll('[role=\"article\"]').length;")
        feed_divs = driver.execute_script("return document.querySelectorAll('div[role=\"feed\"] > div').length;")
        print(f"Scroll {s+1}: height {h_before} -> {h_after} | articles={articles} | feed_divs={feed_divs}")
finally:
    driver.quit()


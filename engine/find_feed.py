import json
import time
import fb_scraper

d = fb_scraper.make_driver()
try:
    fb_scraper.login_with_cookies(d)
    url = "https://www.facebook.com/groups/flatsharemumbai/?sorting_setting=CHRONOLOGICAL"
    d.get(url)
    time.sleep(5)
    print("CURRENT URL:", d.current_url)
    print("TITLE:", d.title)
    
    # Check what role or feed containers Facebook renders
    containers = d.execute_script("""
        const res = {};
        res.feedElements = Array.from(document.querySelectorAll('[role="feed"]')).map(el => ({
            tag: el.tagName,
            classes: el.className,
            childrenCount: el.children.length
        }));
        res.articlesCount = document.querySelectorAll('[role="article"]').length;
        res.mainCount = document.querySelectorAll('[role="main"]').length;
        return res;
    """)
    print("CONTAINERS:", containers)
finally:
    d.quit()


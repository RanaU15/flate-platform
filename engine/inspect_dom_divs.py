import json
import time
import fb_scraper

d = fb_scraper.make_driver()
try:
    fb_scraper.login_with_cookies(d)
    url = "https://www.facebook.com/groups/flatsharemumbai/?sorting_setting=CHRONOLOGICAL"
    d.get(url)
    time.sleep(5)
    
    # Inspect what attributes or selectors exist inside feed > div
    info = d.execute_script("""
        const divs = Array.from(document.querySelectorAll('div[role="feed"] > div'));
        return divs.map(div => {
            const msg = div.querySelector('[data-ad-rendering-role="story_message"]');
            const dirAuto = div.querySelector('div[dir="auto"]');
            const roleArticle = div.querySelector('[role="article"]');
            return {
                textLen: (div.innerText || '').length,
                hasStoryMsg: !!msg,
                hasDirAuto: !!dirAuto,
                hasRoleArticle: !!roleArticle,
                snippet: (div.innerText || '').slice(0, 100).replace(/\\n/g, ' ')
            };
        });
    """)
    print("TOTAL FEED DIVS:", len(info))
    for i, item in enumerate(info[:10], 1):
        print(f"Div {i}: msg={item['hasStoryMsg']} article={item['hasRoleArticle']} textLen={item['textLen']} | {item['snippet']}")
finally:
    d.quit()


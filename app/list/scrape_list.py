from bs4 import BeautifulSoup
from utilities.logger import logger
from app.selenium_setting import *

def get_html(url, cookie):
    browser = open_url(url, window_whosh=False)
    if browser: 
        logger.info(f'scrape_list.py_🟢 Opened {url[:10]}..')
        login(browser, cookie)
        input('scroll down and press enter: ')
        html = browser.page_source
        logout(browser, cookie)
    else:
        logger.error(f'scrape_list.py_🔴 Failed to open {url[:10]}..')
        return None

    return html
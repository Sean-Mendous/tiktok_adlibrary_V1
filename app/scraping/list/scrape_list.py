from bs4 import BeautifulSoup
from utilities.logger import logger
from app.scraping.selenium_setting import *

def get_html(url, cookie):
    try:
        browser = open_url(url, window_whosh=False)
        logger.info(f' >opened {url[:10]}..')
        if not browser:
            raise RuntimeError(f'Failed to open {url[:10]}..')
    except Exception as e:
        raise RuntimeError(f'Failed to open {url[:10]}..: {e}') from e

    try:
        login(browser, cookie)
        logger.info(f' >logged in')
    except Exception as e:
        raise RuntimeError(f'Failed to login: {e}') from e

    try:
        input('scroll down and press enter: ')
        html = browser.page_source
        if not html:
            raise RuntimeError(f'Failed to scrape html')
        logger.info(f' >scraped html')
    except Exception as e:
        raise RuntimeError(f'Failed to scrape html: {e}') from e

    try:    
        logout(browser, cookie)
        logger.info(f' >logged out')
        return html
    except Exception as e:
        raise RuntimeError(f'Failed to open {url[:10]}..: {e}') from e


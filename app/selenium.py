from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import json
from utilities.logger import logger

def open_url(url):
    chrome_options = Options()
    service = Service(ChromeDriverManager().install())
    browser = webdriver.Chrome(service=service, options=chrome_options)
    browser.get(url)
    return browser

def login(browser, cookie):
    with open(cookie, "r") as file:
        cookies = json.load(file)
    for cookie in cookies:
        browser.add_cookie(cookie)
    browser.refresh()
    logger.info(f' >scrape_list.py_Logged in')
    return None

def logout(browser, cookie):
    cookies = browser.get_cookies()
    with open(cookie, "w") as file:
        json.dump(cookies, file)
    browser.quit()
    logger.info(f' >scrape_list.py_Logged out')
    return None
    
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from utilities.logger import logger
from app.selenium_setting import *

def click_button(xpath, browser, sleep=5):
    try:
        button = browser.find_element(By.XPATH, xpath)  
        button.click()
        logger.info(f' -clicked button')
    except Exception as e:
        raise RuntimeError(f' -🔴 could not click button: {e}') from e

    time.sleep(sleep)
    html = browser.page_source
    soup = BeautifulSoup(html, "html.parser")
    text_sections = []

    try:
        text_section_1 = soup.find("p", class_="TopadsDetailPage_metricInfo__L86_t")
        text_sections.append(text_section_1)
        logger.info(f' -got "text_section_1"')
    except Exception as e:
        raise RuntimeError(f' -🔴 could not get "text_section_1": {e}') from e

    try:
        text_section_2 = soup.find("span", class_="TopadsDetailPage_metricRankValue__DnIqe")
        text_sections.append(text_section_2)
        logger.info(f' -got "text_section_2"')
    except Exception as e:
        raise RuntimeError(f' -🔴 could not get "text_section_2": {e}') from e
    
    return text_sections

def get_time_htmls(browser, original_html):
    htmls = {}
    htmls['original'] = original_html
    soup = BeautifulSoup(original_html, "html.parser")
    button_section = soup.find("div", class_="TopadsDetailPage_metricTabs__TVRFV")

    if button_section:
        logger.info(f' -got "button_section"')
        try:
            div_items = button_section.find_all("div", class_="TopadsDetailPage_tab__wvVhL")
            metric_items = [div_items.get_text(strip=True) for div_items in soup.find_all("span", class_="TopadsDetailPage_tabText__2jG0S")]
            for i, item in enumerate(metric_items, start=1):
                html = None
                xpath1 = f'//*[@id="bcModalWrapper"]/div/div/div[2]/div[5]/div[2]/div/div[1]/div[{i}]/span[1]'
                xpath2 = f'//*[@id="bcModalWrapper"]/div/div/div[2]/div[4]/div[2]/div/div[1]/div[{i}]/span[1]'
                
                try:
                    text_section = click_button(xpath1, browser)
                    logger.info(f' -tried xpath1')
                except:
                    text_section = click_button(xpath2, browser)
                    logger.info(f' -tried xpath2')

                if item == 'CTR':
                    htmls['ctr'] = text_section
                    logger.info(f' -got "ctr"')
                    continue
                elif item == 'CVR':
                    htmls['cvr'] = text_section
                    logger.info(f' -got "cvr"')
                    continue
                elif item == 'クリック数' or item == 'Clicks':
                    htmls['clicks'] = text_section
                    logger.info(f' -got "clicks"')
                    continue
                elif item == 'コンバージョン数' or item == 'Conversions':
                    htmls['conversions'] = text_section
                    logger.info(f' -got "conversions"')
                    continue
                elif item == '残存' or item == 'Remain':
                    htmls['remaining'] = text_section
                    logger.info(f' -got "remaining"')
                    continue
                else:
                    continue
        except Exception as e:
            raise RuntimeError(f' -🔴 could not get time data: {e}') from e
    
    return htmls

def get_htmls(url, cookie):
    try:
        browser = open_url(url, window_whosh=True)
        if browser: 
            logger.info(f' >🟢 Successfully opened {url[:10]}..')
        else:
            raise RuntimeError(f' >🔴 could not open {url[:10]}..')
    except Exception as e:
        raise RuntimeError(f' >🔴 could not open {url[:10]}..: {e}') from e

    try:
        login(browser, cookie)
        logger.info(f' >🟢 Successfully logged in')
    except Exception as e:
        raise RuntimeError(f' >🔴 could not login: {e}') from e

    original_html = browser.page_source

    try:
        htmls = get_time_htmls(browser, original_html)
        if htmls:
            logger.info(f' >🟢 Successfully got time data')
        else:
            raise RuntimeError(f' >🔴 could not get time data')
    except Exception as e:
        raise RuntimeError(f' >🔴 could not get time data: {e}') from e

    try:
        logout(browser, cookie)
        logger.info(f' >🟢 Successfully logged out')
    except Exception as e:
        raise RuntimeError(f' >🔴 could not logout: {e}') from e

    return htmls
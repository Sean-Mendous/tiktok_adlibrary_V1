from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time

from utilities.logger import logger
from app.selenium import *

def click_button(xpath, browser, sleep=5):
        button = browser.find_element(By.XPATH, xpath)  
        button.click()
        time.sleep(sleep)
        html = browser.page_source
        soup = BeautifulSoup(html, "html.parser")
        text_sections = []
        text_section_1 = soup.find("p", class_="TopadsDetailPage_metricInfo__L86_t")
        text_section_2 = soup.find("span", class_="TopadsDetailPage_metricRankValue__DnIqe")
        text_sections.append(text_section_1)
        text_sections.append(text_section_2)
        return text_sections

def get_time_htmls(browser, original_html):
    htmls = {}
    htmls['original'] = original_html
    soup = BeautifulSoup(original_html, "html.parser")
    button_section = soup.find("div", class_="TopadsDetailPage_metricTabs__TVRFV")

    if button_section:
        try:
            div_items = button_section.find_all("div", class_="TopadsDetailPage_tab__wvVhL")
            metric_items = [div_items.get_text(strip=True) for div_items in soup.find_all("span", class_="TopadsDetailPage_tabText__2jG0S")]
            for i, item in enumerate(metric_items, start=1):
                html = None
                xpath1 = f'//*[@id="bcModalWrapper"]/div/div/div[2]/div[5]/div[2]/div/div[1]/div[{i}]/span[1]'
                xpath2 = f'//*[@id="bcModalWrapper"]/div/div/div[2]/div[4]/div[2]/div/div[1]/div[{i}]/span[1]'
                
                try:
                    text_section = click_button(xpath1, browser)
                except:
                    text_section = click_button(xpath2, browser)

                if item == 'CTR':
                    htmls['ctr'] = text_section
                    continue
                elif item == 'CVR':
                    htmls['cvr'] = text_section
                    continue
                elif item == 'クリック数' or item == 'Clicks':
                    htmls['clicks'] = text_section
                    continue
                elif item == 'コンバージョン数' or item == 'Conversions':
                    htmls['conversions'] = text_section
                    continue
                elif item == '残存' or item == 'Remain':
                    htmls['remaining'] = text_section
                    continue
                else:
                    continue
        except:
            logger.error(f'scrape_list.py_🔴 could not successfully get time data')
            return None
    
    logger.info(f'scrape_list.py_🟢 successfully got time data')
    return htmls

def get_htmls(url, cookie):
    browser = open_url(url)
    if browser: 
        logger.info(f'scrape_list.py_🟢 Opened {url[:10]}..')
        login(browser, cookie)
        original_html = browser.page_source
        htmls = get_time_htmls(browser, original_html)
        logout(browser, cookie)
    else:
        logger.error(f'scrape_list.py_🔴 Failed to open {url[:10]}..')
        return None

    return htmls
from bs4 import BeautifulSoup
from utilities.logger import logger

def extract_list(html):
    soup = BeautifulSoup(html, "html.parser")
    data = []

    data_section = soup.find("div", class_="CommonGridLayoutDataList_listWrap__aDyjD index-mobile_listWrap__lcrSL TopadsList_topadsDataContentWrap__bZ3dt index-mobile_topadsDataContentWrap__4uruH TopadsList_contentWrapper__yakeY")
    if data_section:
        metric_items = data_section.find_all("div", class_="CommonGridLayoutDataList_cardWrapper__jkA9g TopadsList_cardWrapper__9A7Uf index-mobile_cardWrapper__TEjKX")
        for item in metric_items:
            a_tag = item.find('a')
            if a_tag:
                url = a_tag.get('href')
                if url:
                    data.append(f'https://ads.tiktok.com{url}')
            
    return data

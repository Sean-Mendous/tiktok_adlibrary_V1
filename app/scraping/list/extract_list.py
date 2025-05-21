from bs4 import BeautifulSoup
from utilities.logger import logger

def extract_list(html):
    soup = BeautifulSoup(html, "html.parser")
    data = []

    try:
        data_section = soup.find("div", class_="CommonGridLayoutDataList_listWrap__aDyjD index-mobile_listWrap__lcrSL TopadsList_topadsDataContentWrap__bZ3dt index-mobile_topadsDataContentWrap__4uruH TopadsList_contentWrapper__yakeY")
        if not data_section:
            raise RuntimeError("Missing data section")

        metric_items = data_section.find_all("div", class_="CommonGridLayoutDataList_cardWrapper__jkA9g TopadsList_cardWrapper__9A7Uf index-mobile_cardWrapper__TEjKX")
        if not metric_items:
            raise RuntimeError("No metric items found")

        for item in metric_items:
            a_tag = item.find("a")
            if not a_tag or not a_tag.get("href"):
                raise RuntimeError("URL or a_tag missing in one of the items")

            data.append(f'https://ads.tiktok.com{a_tag.get("href")}')

    except Exception as e:
        raise RuntimeError(f"Failed to extract list: {e}") from e

    return data

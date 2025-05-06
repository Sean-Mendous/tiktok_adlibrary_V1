import re
from bs4 import BeautifulSoup
from utilities.logger import logger

def convert_unit_to_number(string_list):
    if isinstance(string_list, str):
        string_list = [string_list]
    result = []
    for item in string_list:
        item = item.strip()
        try:
            if 'M' in item:
                num = item.replace('M', '')
                result.append(int(float(num) * 1_000_000) if num else 0)
                logger.info(f' -got an expected value: {item}')
            elif 'K' in item:
                num = item.replace('K', '')
                result.append(int(float(num) * 1_000) if num else 0)
                logger.info(f' -got an expected value: {item}')
            else:
                result.append(int(item))
                logger.info(f' -got an expected value: {item}')
        except ValueError:
            result.append(0)
            logger.warning(f' -got an unexpected value')
    return result

def about_section(soup):
    data = {}
    about_section = soup.find("div", class_="TopadsDetailPage_card___PmTU TopadsDetailPage_basicInfoContainer__eHJye")
    if about_section:
        logger.info(f' -got "section"')
        metric_items = about_section.find_all("div", class_="BasicInfoItem_container__pjw4E index-mobile_container__2MFvR TopadsDetailPage_infoItem__vs2lI")
        for item in metric_items:
            label = item.find("span", class_="BasicInfoItem_title__4z_CB")
            if label:
                label_text = label.get_text(strip=True)
            else:
                label_text = None

            value = item.find("span", class_="BasicInfoItem_value__psIua")
            if value:
                value_text = value.get_text(strip=True)
            else:
                value_text = None

            if label_text and value_text:
                if label_text == '産業' or label_text == 'Industry':
                    data['about_industry'] = value_text
                    logger.info(f' -got "about_industry" ({value_text[:5]}...)')
                elif label_text == 'ブランド名' or label_text == 'Brand name':
                    data['about_brand'] = value_text  
                    logger.info(f' -got "about_brand" ({value_text[:5]}...)')
                else:
                    continue

            elif label_text:
                if label_text == 'ランディングページ' or label_text == 'Landing Page':
                    a_tag = item.find("a")
                    if a_tag:
                        data['about_landing'] = a_tag.get("href")
                        logger.info(f' -got "about_landing" ({data["about_landing"][:5]}...)')
                elif label_text == '広告キャプション' or label_text == 'Ad caption':
                    div_tag = item.find("div")
                    if div_tag:
                        data['about_caption'] = div_tag.get_text(strip=True)
                        logger.info(f' -got "about_caption" ({data["about_caption"][:5]}..)')
            else:
                continue
    return data

def data_section(soup):
    data = {}
    data_section = soup.find("div", class_="TopadsDetailPage_metricsContainer__AsSBJ")
    if data_section:
        logger.info(f' -got "section"')
        metric_items = data_section.find_all("div", class_="TopadsDetailPage_metricItem__BzCV3")
        for item in metric_items:
            label = item.find("span", class_="TopadsDetailPage_label__AK0JL")
            if label:
                label_text = label.get_text(strip=True)
            value = item.find("span", class_="TopadsDetailPage_value__8kWUW")
            if value:
                value_text = value.get_text(strip=True)

            if label_text and value_text:
                if label_text == 'いいね' or label_text == 'Likes':
                    data['data_likes'] = convert_unit_to_number(value_text)[0]
                    logger.info(f' -got "data_likes" ({data["data_likes"]})')
                elif label_text == 'コメント' or label_text == 'Comments':
                    data['data_comments'] = convert_unit_to_number(value_text)[0]
                    logger.info(f' -got "data_comments" ({data["data_comments"]})')
                elif label_text == 'シェア数' or label_text == 'Shares':
                    data['data_shares'] = convert_unit_to_number(value_text)[0]
                    logger.info(f' -got "data_shares" ({data["data_shares"]})')
                elif label_text == 'CTR':
                    number = re.findall(r'\d+', value_text)
                    data['data_ctr'] = int(number[0])
                    logger.info(f' -got "data_ctr" ({data["data_ctr"]})')
                elif label_text == '予算' or label_text == 'Budget':
                    data['data_budget'] = value_text
                    logger.info(f' -got "data_budget" ({data["data_budget"][:5]}...)')
                else:
                    continue
            else:
                continue
    return data

def video_section(soup):
    data = {}
    video_section = soup.find("div", class_="xgplayer-container")
    if video_section:
        logger.info(f' -got "section"')
        video_url = video_section.find("video")
        if video_url:
            video_url = video_url.get("src")
            data['video_url'] = video_url
            logger.info(f' -got "video_url" ({data["video_url"][:5]}...)')
    return data

def time_section(htmls):
    def get_text_from_time_data(text_section):
        sec, top = None, None
        for text in text_section:
            soup = BeautifulSoup(str(text), "html.parser")
            peak_span_tags = soup.find_all("p", class_="TopadsDetailPage_metricInfo__L86_t")
            top_span_tags = soup.find_all("span", class_="TopadsDetailPage_metricRankValue__DnIqe")

            if peak_span_tags:
                sec = []
                span_tags = peak_span_tags[0].find_all("span")
                for span in span_tags:
                    number = span.get_text(strip=True)
                    if number.isdigit():
                        number = int(number)
                        sec.append(number)
                        logger.info(f' -got "sec" ({sec})')
                    else:
                        continue

            elif top_span_tags:
                text = top_span_tags[0].get_text(strip=True)
                number = re.findall(r'\d+', text)
                number = number[0]
                if number.isdigit():
                    top = int(number)
                    logger.info(f' -got "top" ({top})')
                else:
                    continue

        if sec and top:
            return sec, top
        else:
            return False
    
    data = {}
    for key, text_section in htmls.items():
        if key == 'ctr' or key == 'CTR':
            logger.info(f' -got "ctr"')
            ctr = get_text_from_time_data(text_section)
            if ctr:
                ctr_sec, ctr_top = ctr
                if ctr_sec:
                    data['time_ctr_sec'] = ctr_sec
                    logger.info(f' -got "time_ctr_sec" ({data["time_ctr_sec"]})')
                if ctr_top:
                    data['time_ctr_top'] = ctr_top
                    logger.info(f' -got "time_ctr_top" ({data["time_ctr_top"]})')
            else:
                continue
        elif key == 'cvr' or key == 'CVR':
            logger.info(f' -got "cvr"')
            cvr = get_text_from_time_data(text_section)
            if cvr:
                cvr_sec, cvr_top = cvr
                if cvr_sec:
                    data['time_cvr_sec'] = cvr_sec
                    logger.info(f' -got "time_cvr_sec" ({data["time_cvr_sec"]})')
                if cvr_top:
                    data['time_cvr_top'] = cvr_top
                    logger.info(f' -got "time_cvr_top" ({data["time_cvr_top"]})')
            else:
                continue
        else:
            continue
    return data

def format_timedata(data):
    formatted_data = {}
    if 'time_ctr_sec' in data and data['time_ctr_sec']:
        time_ctr_sec = data['time_ctr_sec']
        ctr_time_keys = ['time_ctr_sec1', 'time_ctr_sec2', 'time_ctr_sec3']
        ctr_time_data = dict(zip(ctr_time_keys, time_ctr_sec))
        formatted_data.update({
            **ctr_time_data,
        })
        logger.info(f' -fixed "time_ctr_sec"')

    if 'time_cvr_sec' in data and data['time_cvr_sec']:
        time_cvr_sec = data['time_cvr_sec']
        cvr_time_keys = ['time_cvr_sec1', 'time_cvr_sec2', 'time_cvr_sec3']
        cvr_time_data = dict(zip(cvr_time_keys, time_cvr_sec))
        formatted_data.update({
            **cvr_time_data,
        })
        logger.info(f' -fixed "time_cvr_sec"')

    if 'time_ctr_top' in data and data['time_ctr_top']:
        formatted_data['time_ctr_top'] = data['time_ctr_top']
        logger.info(f' -fixed "time_ctr_top"')

    if 'time_cvr_top' in data and data['time_cvr_top']:
        formatted_data['time_cvr_top'] = data['time_cvr_top']
        logger.info(f' -fixed "time_cvr_top"')

    return formatted_data

def extract_indivisual(htmls):
    data = {}
    original_soup = BeautifulSoup(htmls['original'], "html.parser")

    try:
        about_data = about_section(original_soup)
        if about_data:
            logger.info(f' >🟢 Successfully got "about_data"')
        else:
            raise RuntimeError(f' >🔴 did not get "about_data"')
    except Exception as e:
        logger.error(f' >🔴 error "about_section": {e}')
        about_data = {}

    try:
        data_data = data_section(original_soup)
        if data_data:
            logger.info(f' >🟢 Successfully got "data_data"')
        else:
            raise RuntimeError(f' >🔴 did not get "data_data"')
    except Exception as e:
        logger.error(f' >🔴 error "data_section": {e}')
        data_data = {}

    try:
        video_data = video_section(original_soup)
        if video_data:
            logger.info(f' >🟢 Successfully got "video_data"')
        else:
            raise RuntimeError(f' >🔴 did not get "video_data"')
    except Exception as e:
        logger.error(f' >🔴 error "video_section": {e}')
        video_data = {}

    try:
        time_data = time_section(htmls)
        if time_data:
            logger.info(f' >🟢 Successfully got "time_data"')
        else:
            raise RuntimeError(f' >🔴 did not get "time_data"')
    except Exception as e:
        logger.error(f' >🔴 error "time_section": {e}')
        time_data = {}
    
    try:
        formatted_time_data = format_timedata(time_data)
        if formatted_time_data:
            logger.info(f' >🟢 Successfully formatted "time_data"')
        else:
            raise RuntimeError(f' >🔴 could not format "time_data"')
    except Exception as e:
        logger.error(f' >🔴 error "format_timedata": {e}')
        formatted_time_data = {}

    data = about_data | data_data | video_data | formatted_time_data
    return data

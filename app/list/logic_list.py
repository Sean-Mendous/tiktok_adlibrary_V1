import time
from utilities.google_spreadsheet import *
from utilities.save_file import *
from utilities.logger import logger
from app.list.scrape_list import get_html
from app.list.extract_list import extract_list

def run_flow(start_row, end_row, cookie, output_path, spreadsheet):
    sheet_id = spreadsheet["sheet_id"]
    sheet_1 = spreadsheet["sheet_1"]
    sheet_original_2 = spreadsheet["sheet_2"]
    column_map_1 = spreadsheet["column_map_1"]
    column_map_2 = spreadsheet["column_map_2"]
    headder_1 = column_map_1["headder"]
    headder_2 = column_map_2["headder"]
    credentials_path = spreadsheet["credentials_path"]

    sheet_1 = certification_google_spreadsheet(sheet_id, sheet_1, credentials_path)
    if sheet_1:
        logger.info(f'logic_list.py_🟢 Successfully got sheet_1')
    else:
        logger.error(f'logic_list.py_🔴 Failed to get sheet_1')
        return False
    
    try:
        input_multi_data_1 = input_google_spreadsheet_multi(sheet_1, column_map_1, start_row, end_row)
        if input_multi_data_1:
            logger.info(f'logic_list.py_🟢 Successfully got sheet_1')
        else:
            logger.error(f'logic_list.py_🔴 Failed to get sheet_1')
            return False
    except Exception as e:
        logger.error(f'logic_list.py_🔴 Failed to get sheet_1: {e}')
        return False

    row_1 = start_row
    for data in input_multi_data_1:
        logger.info(f"======start #{row_1}=======")
        sheet_2_name = data["system_name"]
        url = data["system_url"]
        status = data["system_status"]

        if not sheet_2_name:
            logger.info(f"logic_list.py_🟡 #{row_1} does not have a name")
            row_1 += 1
            continue

        if not url:
            logger.info(f"logic_list.py_🟡 #{row_1} does not have a url")
            row_1 += 1
            continue

        if status == 'pending' or status == 'completed':
            logger.info(f"logic_list.py_🟡 #{row_1} is already completed")
            row_1 += 1
            continue

        sheet_2 = duplicate_google_sheet(sheet_id, sheet_original_2, credentials_path, sheet_2_name)
        if sheet_2:
            logger.info(f'logic_list.py_🟢 Successfully duplicated sheet_2')
        else:
            logger.error(f'logic_list.py_🔴 Failed to duplicate sheet_2')
            return False

        html = get_html(url, cookie)
        if html:
            logger.info(f'logic_list.py_🟢 Successfully got html from {url[:10]}..')
        else:
            logger.error(f'logic_list.py_🔴 Failed to get html from {url[:10]}..')
            return False
            
        url_list = extract_list(html)
        if url_list:
            logger.info(f'logic_list.py_🟢 Successfully extracted "{len(url_list)}" urls')
        else:
            logger.error(f'logic_list.py_🔴 Failed to extract urls')
            return False
            
        output_data_2 = []
        for i, url in enumerate(url_list, start=1):
            data = {
                "system_num": f"i",
                "system_status": 'pending',
                "system_url": url
            }
            output_data_2.append(data)
            
        list_output_path = f"{output_path}/sheet_{sheet_2_name}/scrape_list.json"
        save_status = save_by_json(output_data_2, list_output_path)
        if save_status == True:
            logger.info(f'logic_list.py_🟢 Successfully saved data')
        else:
            logger.error(f'logic_list.py_🔴 Failed to save data')
            return False

        try:
            row_2 = headder_2
            output_status = output_google_spreadsheet_multi(sheet_2, column_map_2, row_2, output_data_2)
            if output_status == True:
                logger.info(f'logic_list.py_🟢 Successfully outputted datas')
            elif output_status == False:
                logger.error(f'logic_list.py_🔴 Failed to output datas')
        except Exception as e:
            logger.error(f'logic_list.py_🔴 Failed to output datas: {e}')
            return False
        
        try:
            output_status_1 = {}
            output_status_1["system_status"] = 'pending'
            output_status = output_google_spreadsheet(sheet_1, column_map_1, row_1, output_status_1)
            if output_status == True:
                logger.info(f'logic_list.py_🟢 Successfully outputted status for sheet {sheet_2_name}')
            elif output_status == False:
                logger.error(f'logic_list.py_🔴 Failed to output status for sheet {sheet_2_name}')
        except Exception as e:
            logger.error(f'logic_list.py_🔴 Failed to output status for sheet {sheet_2_name}: {e}')
            return False

        logger.info(f"======end #{row_1}=======")
        row_1 += 1

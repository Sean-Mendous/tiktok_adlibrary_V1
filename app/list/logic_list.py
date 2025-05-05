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
    
    for row_1 in range(start_row, end_row):
        time.sleep(1)
        logger.info(f"======start #{row_1}=======")
        input_data_1 = input_google_spreadsheet(sheet_1, column_map_1, row_1)
        sheet_2_name = input_data_1["system_name"]
        url = input_data_1["system_url"]
        status = input_data_1["system_status"]

        if not sheet_2_name:
            logger.info(f"logic_list.py_🟡 #{row_1} does not have a name")
            continue

        if not url:
            logger.info(f"logic_list.py_🟡 #{row_1} does not have a url")
            continue

        if status == 'pending' and status == 'completed':
            logger.info(f"logic_list.py_🟡 #{row_1} is already completed")
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
                "system_num": f"{i:03}",
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
            for data in output_data_2:
                output_status = output_google_spreadsheet(sheet_2, column_map_2, row_2, data)
                if output_status == True:
                    logger.info(f'logic_list.py_🟢 Successfully outputted for data[{data["system_num"]}]:\n{data}')
                elif output_status == False:
                    logger.error(f'logic_list.py_🔴 Failed to output for data[{data["system_num"]}]:\n{data}')
                row_2 += 1
                time.sleep(1)
                continue
        except Exception as e:
            logger.error(f'logic_list.py_🔴 Failed to output for #{row_1}: {e}')
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

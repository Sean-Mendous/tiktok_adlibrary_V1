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

    try:
        sheet_1 = certification_google_spreadsheet(sheet_id, sheet_1, credentials_path)
        if sheet_1:
            logger.info(f'01-01/02: 🟢 Successfully got certification for sheet_1')
        else:
            return RuntimeError(f'Failed to get certification for sheet_1')
    except Exception as e:
        raise RuntimeError(f'Failed to get certification for sheet_1: {e}') from e

    try:
        input_multi_data_1 = input_google_spreadsheet_multi(sheet_1, column_map_1, start_row, end_row)
        if input_multi_data_1:
            logger.info(f'01-02/02: 🟢 Successfully input data for sheet_1')
        else:
            raise RuntimeError(f'Failed to input data for sheet_1')
    except Exception as e:
        raise RuntimeError(f'Failed to input data for sheet_1: {e}') from e

    row_1 = start_row
    for data in input_multi_data_1:
        logger.info(f"==starting for #{row_1}===")

        sheet_2_name = data["system_name"]
        url = data["system_url"]
        status = data["system_status"]

        if not sheet_2_name:
            logger.warning(f"🟡 does not have a name. Going to next row.")
            row_1 += 1
            continue

        if not url:
            logger.warning(f"🟡 #{row_1} does not have a url. Going to next row.")
            row_1 += 1
            continue

        if status == 'pending' or status == 'completed' or status.isdigit():
            logger.warning(f"🟡 #{row_1} is already completed. Going to next row.")
            row_1 += 1
            continue

        try:
            sheet_2 = duplicate_google_sheet(sheet_id, sheet_original_2, credentials_path, sheet_2_name)
            if sheet_2:
                logger.info(f'02-01/06: 🟢 Successfully duplicated sheet_2')
            else:
                raise RuntimeError(f'Failed to duplicate sheet_2') from e
        except Exception as e:
            raise RuntimeError(f'Failed to duplicate sheet_2: {e}') from e

        try:
            html = get_html(url, cookie)
            if html:
                logger.info(f'02-02/06: 🟢 Successfully got html from {url[:10]}..')
            else:
                raise RuntimeError(f'Failed to get html from {url[:10]}..') from e
        except Exception as e:
            raise RuntimeError(f'Failed to get html from {url[:10]}..: {e}') from e
            
        
        try:
            url_list = extract_list(html)
            if url_list:
                logger.info(f'02-03/06: 🟢 Successfully extracted "{len(url_list)}" urls')
            else:
                raise RuntimeError(f'Failed to extract urls') from e
        except Exception as e:
            raise RuntimeError(f'Failed to extract urls: {e}') from e
            
        output_data_2 = []
        for i, url in enumerate(url_list, start=1):
            data = {
                "system_num": i,
                "system_status": 'pending',
                "system_url": url
            }
            output_data_2.append(data)
        
        alldone_data = {
            "system_num": 9999,
            "system_status": 'all-done',
            "system_url": '-'
        }
        output_data_2.append(alldone_data)
            
        try:
            list_output_path = f"{output_path}/sheet_{sheet_2_name}/scrape_list.json"
            save_status = save_by_json(output_data_2, list_output_path)
            if save_status == True:
                logger.info(f'02-04/06: 🟢 Successfully saved data to json')
            else:
                raise RuntimeError(f'Failed to save data') from e
        except Exception as e:
            raise RuntimeError(f'Failed to save data: {e}') from e

        try:
            row_2 = headder_2
            output_status = output_google_spreadsheet_multi(sheet_2, column_map_2, row_2, output_data_2)
            if output_status == True:
                logger.info(f'02-05/06: 🟢 Successfully outputted datas')
            else:
                raise RuntimeError(f'Failed to output datas') from e
        except Exception as e:
            raise RuntimeError(f'Failed to output datas: {e}') from e
        
        try:
            output_status_1 = {}
            output_status_1["system_status"] = 'pending'
            output_status = output_google_spreadsheet(sheet_1, column_map_1, row_1, output_status_1)
            if output_status == True:
                logger.info(f'02-06/06: 🟢 Successfully outputted status for sheet {sheet_2_name}')
            else:
                raise RuntimeError(f'Failed to output status for sheet {sheet_2_name}') from e
        except Exception as e:
            raise RuntimeError(f'Failed to output status for sheet {sheet_2_name}: {e}') from e

        logger.info(f"==ending for #{row_1}===")
        row_1 += 1

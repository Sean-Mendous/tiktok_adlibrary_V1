import time
from utilities.google_spreadsheet import *
from utilities.save_file import *
from utilities.logger import logger
from app.indivisual.scrape_indivisual import get_htmls
from app.indivisual.extract_indivisual import extract_indivisual

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
        logger.error(f'logic_indivisual.py_🔴 Failed to get sheet_1: {e}')
        return False

    row_1 = start_row
    for data1 in input_multi_data_1:
        logger.info(f"======start #{row_1}=======")
        sheet_2_name = data1["system_name"]
        sheet_1_status = data1["system_status"]

        if not sheet_2_name:
            logger.info(f"logic_list.py_🔴 #{row_1} does not have a name")
            return False
        
        if sheet_1_status == 'completed':
            logger.info(f"logic_list.py_🟡 #{row_1} is already completed")
            row_1 += 1
            continue
        elif sheet_1_status == 'pending':
            logger.info(f'logic_list.py_🟢 #{row_1} starting from beginning')
            first_row = headder_2
        elif sheet_1_status.isdigit():
            sheet_1_status = int(sheet_1_status)
            logger.info(f'logic_list.py_🟢 #{row_1} starting from row {sheet_1_status}')
            first_row = sheet_1_status
        else:
            logger.info(f'logic_list.py_🔴 #{row_1} is not a valid status')
            return False

        try:
            sheet_2 = certification_google_spreadsheet(sheet_id, sheet_2_name, credentials_path)
            if not sheet_2:
                logger.error(f'logic_list.py_🔴 cannot find sheet_2')
                return False
            input_multi_data_2 = input_google_spreadsheet_multi(sheet_2, column_map_2, first_row, 1000)
            if input_multi_data_2:
                logger.info(f'logic_list.py_🟢 Successfully got sheet_2')
            else:
                logger.error(f'logic_list.py_🔴 Failed to get sheet_2')
                return False
        except Exception as e:
            logger.error(f'logic_indivisual.py_🔴 Failed to get sheet_2: {e}')
            return False
        
        row_2 = first_row
        for data2 in input_multi_data_2:
            logger.info(f"-start #{row_2}-")
            url = data2["system_url"]
            num = data2["system_num"]
            sheet_2_status = data2["system_status"]

            if not url:
                logger.info(f"logic_list.py_🔴 #{row_2} does not have a url")
                return False

            if not num:
                logger.info(f"logic_list.py_🔴 #{row_2} does not have a number")
                return False
            
            if sheet_2_status == 'completed':
                logger.info(f"logic_list.py_🟡 #{row_2} is already completed")
                row_2 += 1
                continue

            MAX_RETRIES = 5
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    htmls = get_htmls(url, cookie)
                    if htmls:
                        logger.info(f'logic_list.py_🟢 Successfully got html from {url[:10]}.. (Attempt {attempt})')
                        break
                except Exception as e:
                    logger.warning(f'logic_list.py_🟡 Attempt {attempt} failed to get html from {url[:10]}..: {e}')
                    time.sleep(1)
            else:
                logger.error(f'logic_list.py_🔴🟡 All {MAX_RETRIES} attempts failed to get html from {url[:10]}..')
                row_2 += 1
                continue

            output_data_2 = extract_indivisual(htmls)
            if not output_data_2:
                logger.error(f'logic_indivisual.py_🔴 Failed to extract data')
                return False

            logger.info(f'logic_list.py_🟢 Successfully extracted data')
            indivisual_output_path = f"{output_path}/sheet_{sheet_2_name}/{num}/scrape_indivisual.json"
            save_status = save_by_json(output_data_2, indivisual_output_path)
            if save_status == True:
                logger.info(f'logic_list.py_🟢 Successfully saved data')
            else:
                logger.error(f'logic_list.py_🔴 Failed to save data')
                return False

            try:
                output_status = output_google_spreadsheet(sheet_2, column_map_2, row_2, output_data_2)
                if output_status == True:
                    logger.info(f'logic_list.py_🟢 Successfully outputted for data:\n{output_data_2}')
                elif output_status == False:
                    logger.error(f'logic_list.py_🔴 Failed to output for data:\n{output_data_2}')
                    return False
            except Exception as e:
                logger.error(f'logic_list.py_🔴 Failed to output for data:\n{output_data_2}: {e}')
                return False
        
            try:
                output_status_2 = {}
                output_status_2["system_status"] = 'completed'
                output_status = output_google_spreadsheet(sheet_2, column_map_2, row_2, output_status_2)
                if output_status == True:
                    logger.info(f'logic_list.py_🟢 Successfully outputted status for row {row_2}')
                elif output_status == False:
                        logger.error(f'logic_list.py_🔴 Failed to output status for row {row_2}')
                        return False
            except Exception as e:
                logger.error(f'logic_list.py_🔴 Failed to output status for row {row_2}: {e}')
                return False
        
            try:
                output_status_1 = {}
                output_status_1["system_status"] = row_2 + 1
                output_status = output_google_spreadsheet(sheet_1, column_map_1, row_1, output_status_1)
                if output_status == True:
                    logger.info(f'logic_list.py_🟢 Successfully outputted for sheet {sheet_2_name}')
                elif output_status == False:
                    logger.error(f'logic_list.py_🔴 Failed to output for sheet {sheet_2_name}')
                    return False
            except Exception as e:
                logger.error(f'logic_list.py_🔴 Failed to output for sheet {sheet_2_name}: {e}')
                return False
            
            logger.info(f"-end #{row_2}-")
            row_2 += 1
        
    try:
        output_status_1 = {}
        output_status_1["system_status"] = 'completed'
        output_status = output_google_spreadsheet(sheet_1, column_map_1, row_1, output_status_1)
        if output_status == True:
            logger.info(f'logic_list.py_🟢 Successfully outputted for sheet {sheet_2_name}')
        elif output_status == False:
            logger.error(f'logic_list.py_🔴 Failed to output for sheet {sheet_2_name}')
            return False
    except Exception as e:
        logger.error(f'logic_list.py_🔴 Failed to output for sheet {sheet_2_name}: {e}')
        return False

    logger.info(f"======end #{row_1}=======")

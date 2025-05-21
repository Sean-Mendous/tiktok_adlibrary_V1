import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from utilities.google_spreadsheet import *
from utilities.save_file import *
from utilities.logger import logger
from app.scraping.indivisual.scrape_indivisual import get_htmls
from app.scraping.indivisual.extract_indivisual import extract_indivisual

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
            raise RuntimeError(f'Failed to get certification for sheet_1')
    except Exception as e:
        raise RuntimeError(f'Failed to get certification for sheet_1: {e}') from e
    
    try:
        input_multi_data_1 = input_google_spreadsheet_multi(sheet_1, column_map_1, start_row, end_row)
        if input_multi_data_1:
            logger.info(f'01-02/02: 🟢 Successfully got multi_input for sheet_1')
        else:
            raise RuntimeError(f'Failed to get multi_input for sheet_1')
    except Exception as e:
        raise RuntimeError(f'Failed to get multi_input for sheet_1: {e}') from e

    row_1 = start_row
    for data1 in input_multi_data_1:
        logger.info(f"==starting for #{row_1}/{end_row}==")
        sheet_2_name = data1["system_name"]
        sheet_1_status = data1["system_status"]

        if not sheet_2_name:
            raise RuntimeError(f"#{row_1} does not have a name")

        if sheet_1_status == 'completed':
            logger.warning(f"🟡 #{row_1} is already completed")
            row_1 += 1
            continue
        elif sheet_1_status == 'pending':
            logger.info(f'02-01/04: 🟢 #{row_1} starting from beginning')
            first_row = headder_2
        elif sheet_1_status.isdigit():
            sheet_1_status = int(sheet_1_status)
            logger.info(f'02-02/04: 🟢 #{row_1} starting from {sheet_1_status}')
            first_row = sheet_1_status
        else:
            raise RuntimeError(f'#{row_1} is not a valid status')

        try:
            sheet_2 = certification_google_spreadsheet(sheet_id, sheet_2_name, credentials_path)
            if sheet_2:
                logger.info(f'02-03/04: 🟢 Successfully got certification for sheet_2')
            else:
                raise RuntimeError(f'Failed to get certification for sheet_2')
        except Exception as e:
            raise RuntimeError(f'Failed to get certification for sheet_2: {e}') from e

        try:
            input_multi_data_2 = input_google_spreadsheet_multi(sheet_2, column_map_2, first_row, 1000)
            if input_multi_data_2:
                logger.info(f'02-04/04: 🟢 Successfully got multi_input for sheet_2')
            else:
                raise RuntimeError(f'Failed to get multi_input for sheet_2')
        except Exception as e:
            raise RuntimeError(f'Failed to get multi_input for sheet_2: {e}') from e
        
        row_2 = first_row
        error_count = 0
        for data2 in input_multi_data_2:
            print(f"\nerror_count: {error_count}")
            print(f"(now on sleep..)\n")
            time.sleep(5)
            
            if error_count:
                if error_count > 5:
                    raise RuntimeError(f'🔴 #{row_1}: got failed multi times')
                try:
                    output_status_2 = {}
                    output_status_2["system_status"] = 'error'
                    output_status = output_google_spreadsheet(sheet_2, column_map_2, row_2, output_status_2)
                    if output_status == True:
                        logger.info(f'03 - 00/00: 🟢 Successfully outputted status for sheet_2')
                    else:
                        raise RuntimeError(f'Failed to output status for sheet_2')
                except Exception as e:
                    raise RuntimeError(f'Failed to output status for sheet_2: {e}') from e
                row_2 += 1

            logger.info('')
            logger.info(f"🔄 ~starting for #{row_1} - {row_2}~ 🔄")
            url = data2["system_url"]
            num = data2["system_num"]
            sheet_2_status = data2["system_status"]

            if not url:
                logger.error(f'🔴 {row_2}: does not have a url')
                error_count += 1
                continue

            if not num:
                logger.error(f'🔴 {row_2}: does not have a number')
                error_count += 1
                continue
            
            if sheet_2_status == 'completed':
                logger.warning(f"🟡 {row_2}: is already completed")
                row_2 += 1
                continue

            if sheet_2_status == 'all-done':
                logger.info(f"🟢 {row_1}: Detected all-done")
                break

            MAX_RETRIES = 5
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    htmls = get_htmls(url, cookie)
                    if htmls:
                        logger.info(f'03-01/06: 🟢 Successfully got html from {url[:10]}.. (Attempt {attempt})')
                        break
                    else:
                        logger.warning(f'🟡 Attempt {attempt} failed to get html from {url[:10]}..')
                        time.sleep(1)
                except Exception as e:
                    logger.warning(f'🟡 Attempt {attempt} failed to get html from {url[:10]}..: {e}')
                    time.sleep(1)
            else:
                logger.error(f'🔴 {row_2}: Failed to get html from {url[:10]}..')
                error_count += 1
                continue
            
            try:
                output_data_2 = extract_indivisual(htmls)
                if output_data_2:
                    logger.info(f'03-02/06: 🟢 Successfully extracted data from {url[:10]}..')
                else:
                    logger.error(f'🔴 {row_2}: Failed to extract data from {url[:10]}..')
                    error_count += 1
                    continue
            except Exception as e:
                logger.error(f'🔴 {row_2}: Failed to extract data from {url[:10]}..: {e}')
                error_count += 1
                continue
            
            try:
                indivisual_output_path = f"{output_path}/sheet_{sheet_2_name}/{num:03}/scrape_indivisual.json"
                save_status = save_by_json(output_data_2, indivisual_output_path)
                if save_status == True:
                    logger.info(f'03-03/06: 🟢 Successfully saved data to json')
                else:
                    logger.error(f'🔴 {row_2}: Failed to save data to json')
                    error_count += 1
                    continue
            except Exception as e:
                logger.error(f'🔴 {row_2}: Failed to save data to json: {e}')
                error_count += 1
                continue

            try:
                output_status = output_google_spreadsheet(sheet_2, column_map_2, row_2, output_data_2)
                if output_status == True:
                    logger.info(f'03-04/06: 🟢 Successfully outputted for data:\n{output_data_2}')
                else:
                    logger.error(f'🔴 {row_2}: Failed to output for data:\n{output_data_2}')
                    error_count += 1
                    continue
            except Exception as e:
                logger.error(f'🔴 {row_2}: Failed to output for data:\n{output_data_2}: {e}')
                error_count += 1
                continue
        
            try:
                output_status_2 = {}
                output_status_2["system_status"] = 'completed'
                output_status = output_google_spreadsheet(sheet_2, column_map_2, row_2, output_status_2)
                if output_status == True:
                    logger.info(f'03-05/06: 🟢 Successfully outputted status for sheet_2')
                else:
                    logger.error(f'🔴 {row_2}: Failed to output status for sheet_2')
                    error_count += 1
                    continue
            except Exception as e:
                logger.error(f'🔴 {row_2}: Failed to output status for sheet_2: {e}')
                error_count += 1
                continue
        
            try:
                output_status_1 = {}
                output_status_1["system_status"] = row_2 + 1
                output_status = output_google_spreadsheet(sheet_1, column_map_1, row_1, output_status_1)
                if output_status == True:
                    logger.info(f'03-06/06: 🟢 Successfully outputted status for sheet_1')
                else:
                    logger.error(f'🔴 {row_1}: Failed to output status for sheet_1')
                    error_count += 1
                    continue
            except Exception as e:
                logger.error(f'🔴 {row_1}: Failed to output status for sheet_1: {e}')
                error_count += 1
                continue
            
            logger.info(f"🔄 ~ending for #{row_1} - {row_2}~ 🔄")
            error_count = 0
            row_2 += 1
        
        try:
            output_status_1 = {}
            output_status_1["system_status"] = 'completed'
            output_status = output_google_spreadsheet(sheet_1, column_map_1, row_1, output_status_1)
            if output_status == True:
                logger.info(f'04 - 01/01: 🟢 Successfully outputted final status for sheet_1')
            else:
                raise RuntimeError(f'🔴 {row_1}: Failed to output final status for sheet_1')
        except Exception as e:
            raise RuntimeError(f'🔴 {row_1}: Failed to output final status for sheet_1: {e}') from e

        logger.info(f"==ending for #{row_1}/{end_row}==\n\n")
        row_1 += 1

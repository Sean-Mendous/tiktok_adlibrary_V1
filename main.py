import argparse
import json
import os
import sys
import traceback
from utilities.logger import logger

logger.info("0000: 🏃‍♀️🏃‍♀️ Starting main execution 🏃‍♀️🏃‍♀️")

logger.info("0100: Parsing arguments")
parser = argparse.ArgumentParser(description="Run form submission with client config and row range.")
parser.add_argument("--client", required=True, help="Client folder name (e.g., client_a)")
parser.add_argument("--system", required=True, help="system number (e.g., list=1, indivisual=2)")
parser.add_argument("--start_row", required=True, help="start row number (e.g., 1)")
parser.add_argument("--end_row", required=True, help="end row number (e.g., 1)")
args = parser.parse_args()

logger.info("0200: Checking client folder")
client_path = f"clients/{args.client}"
if not os.path.exists(client_path):
    logger.error(f"E0201: 🔴 cannot find client folder for @{args.client}")
    traceback.print_exc()
    sys.exit(1)
client_config_path = f"{client_path}/googlesheet_config.json"
if not os.path.exists(client_config_path):
    logger.error(f"E0202: 🔴 cannot find client googlesheet-config for @{args.client}")
    traceback.print_exc()
    sys.exit(1)
else:
    logger.info(f"0300: found client folder for @{args.client}")
    with open(client_config_path) as f:
        spreadsheet = json.load(f)
client_output_path = f"{client_path}/output"

logger.info("0400: Parsing system number")
system_num = int(args.system)
start_row = int(args.start_row)
end_row = int(args.end_row)
cookie = 'cookies.json'

logger.info("0500: Running system")
if system_num == 1:
    try:
        from app.list.logic_list import run_flow
        run_flow(start_row, end_row, cookie, client_output_path, spreadsheet)
        logger.info(f"0600: 🟢 success for @{args.client}")
    except Exception as e:
        logger.critical(f"C0600: 🔴 error occurred while running system: {e}")
        traceback.print_exc()
        sys.exit(2)
elif system_num == 2:
    try:
        from app.indivisual.logic_indivisual import run_flow
        run_flow(start_row, end_row, cookie, client_output_path, spreadsheet)
        logger.info(f"0600: 🟢 success for @{args.client}")
    except Exception as e:
        logger.critical(f"C0600: 🔴 error occurred while running system: {e}")
        traceback.print_exc()
        sys.exit(2)
else:
    logger.error(f"E0501: 🔴 system number {system_num} is not valid")
    traceback.print_exc()
    sys.exit(1)

logger.info("0700: 🍺🍺 main execution completed 🍺🍺")

"""
python main.py --client client_test --start_row 3 --end_row 6 --system 1
python main.py --client client_test --start_row 4 --end_row 6 --system 2
"""






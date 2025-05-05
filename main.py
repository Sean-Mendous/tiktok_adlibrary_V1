import argparse
import json
import os
from utilities.logger import logger

cookie = 'cookies.json'

parser = argparse.ArgumentParser(description="Run form submission with client config and row range.")
parser.add_argument("--client", required=True, help="Client folder name (e.g., client_a)")
parser.add_argument("--system", required=True, help="system number (e.g., list=1, indivisual=2)")
parser.add_argument("--start_row", required=True, help="start row number (e.g., 1)")
parser.add_argument("--end_row", required=True, help="end row number (e.g., 1)")
args = parser.parse_args()

client_path = f"clients/{args.client}"
client_config_path = f"{client_path}/googlesheet_config.json"
client_output_path = f"{client_path}/output"
if not os.path.exists(client_config_path):
    logger.info(f"main.py_🔴 config.json not found for @{args.client}")
    exit(1)
with open(client_config_path) as f:
    spreadsheet = json.load(f)

system_num = int(args.system)
start_row = int(args.start_row)
end_row = int(args.end_row)

if system_num == 1:
    from app.list.logic_list import run_flow
    status = run_flow(start_row, end_row, cookie, client_output_path, spreadsheet)
elif system_num == 2:
    from app.indivisual.logic_indivisual import run_flow
    status = run_flow(start_row, end_row, cookie, client_output_path, spreadsheet)
else:
    logger.info(f"main.py_🔴 system number is not valid")
    exit(1)

if status == False:
    logger.info(f"main.py_🔴 failed for @{args.client}")
else:
    logger.info(f"main.py_🟢 success for @{args.client}")

"""
python main.py --client client_test --start_row 3 --end_row 6 --system 1
python main.py --client client_test --start_row 3 --end_row 6 --system 2
"""






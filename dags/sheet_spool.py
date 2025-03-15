import gspread
import pandas as pd
gc = gspread.service_account(filename="C:/Users/HP/materials/jireh-ope-9348c4d91fd9.json")

#insert your sheet id in the middle of the string exclamation in the next line; sheet id is the id after; https://docs.google.com/spreadsheets/d/
spreadsheet_id = ""
sh = gc.open_by_key(spreadsheet_id)

worksheet = sh.sheet1  

data = worksheet.get_all_values()

for row in data:
    print(row)
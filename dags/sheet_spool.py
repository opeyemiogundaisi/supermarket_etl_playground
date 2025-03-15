import gspread
from google.oauth2.service_account import Credentials
import json
import os

def upload_to_googlesheet(spreadsheet_id):
    try:
        print(f"Attempting to connect to spreadsheet with ID: {spreadsheet_id}")
        
        json_path = "C:/Users/HP/materials/jireh-ope-9348c4d91fd9.json"
        if not os.path.exists(json_path):
            print(f"ERROR: Credentials file not found at {json_path}")
            return []
        
        # Connect to Google Sheets with error handling
        try:
            gc = gspread.service_account(filename=json_path)
            print("Successfully authenticated with Google Sheets")
        except Exception as e:
            print(f"Authentication error: {str(e)}")
            return []
        
        try:
            sh = gc.open_by_key(spreadsheet_id)
            print(f"Successfully opened spreadsheet: {sh.title}")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"ERROR: Spreadsheet with ID {spreadsheet_id} not found")
            return []
        except Exception as e:
            print(f"Error opening spreadsheet: {str(e)}")
            return []
        
        try:
            worksheet = sh.sheet1
            print(f"Accessed worksheet: {worksheet.title}")
        except Exception as e:
            print(f"Error accessing worksheet: {str(e)}")
            return []
        
        try:
            data = worksheet.get_all_values()
            print(f"Retrieved {len(data)} rows of data")
            
            for i, row in enumerate(data[:3]):
                print(f"Row {i}: {row}")
            
            if len(data) <= 1:
                print("WARNING: Only header row or empty data found")
        except Exception as e:
            print(f"Error getting values: {str(e)}")
            return []
        
        if len(data) > 0:
            headers = data[0]
            print(f"Headers: {headers}")
            
            rows = []
            for row in data[1:]:
                row_dict = {headers[i]: value for i, value in enumerate(row) if i < len(headers)}
                rows.append(row_dict)
            
            print(f"Converted {len(rows)} rows to dictionary format")
            return rows
        else:
            print("No data found in the spreadsheet")
            return []
            
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return []


if __name__ == "__main__":
    test_spreadsheet_id = "1RoB52Rk-71uQiuplol7yhLvgWHFv3v079u-yMEQEu8o"
    result = upload_to_googlesheet(test_spreadsheet_id)
    print(f"Final result: {len(result)} rows returned")
    if result:
        print("First row example:", result[0])
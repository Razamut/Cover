import os
import json
import gspread
from pandas import DataFrame as DF
from datetime import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials

oPath = os.path.join('.', "data")
if not os.path.exists(oPath):
    os.mkdir(oPath)
loan_cols = ["user_id","loan_id","loan_type","amt_requested","loan_amt_usd",
             "loan_amt_ld","rate","interest_ld","interest_usd","duration_month",
             "status","take_out_dt","loan_plus_intr_ld","loan_plus_intr_usd"]

connections_cols = ["user_id","employment","employer_name","occupation","monthly_salary",
                    "mobile_type","marital_status","spouse_first_name","spouse_last_name",
                    "spouse_age","spouse_employement_status","home_address",
                    "business_address","employer_address","business_description",
                    "fb_acct","housing_type","rent_amt","family_size","n_kids",
                    "kids_sch_type","spouse_employed"]

users_cols = ["user_id","first_name","last_name","middle_name","id_number","id_type",
              "age","gender","ph_numbers","email"]

collections_cols = ["user_id","loan_id","loan_plus_intr_usd","loan_plus_intr_ld",
                    "payment_dt","payment_amt_usd","payment_amt_ld"]

gcp_cred_file = 'config/gcp_credentials.json'
gsheet_key_file = "config/gsheet_key.json"
# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(gcp_cred_file, scope)
gc = gspread.authorize(creds)

sheets = {'loans': 0,
        'collections': 1,
        'users': 2,
        'connections': 3}

columns = {'loans': loan_cols,
          'collections': collections_cols,
          'users': users_cols,
          'connections': connections_cols}

def load_json(json_file_path):
    with open(json_file_path, encoding='utf-8') as json_file:
        json_data = json.load(json_file)
    return json_data
# Find a workbook by key
key_map = load_json(gsheet_key_file)
wb = gc.open_by_key(key_map.get("key"))
# get all sheets from workbook
for sheetName in sheets:
    sheetNbr = sheets[sheetName]
    sheet = wb.get_worksheet(sheetNbr)
    records = sheet.get_all_records()
    df = DF(records)
    df = df[columns[sheetName]]
    fileName = sheetName + '.csv'
    filePath = os.path.join(oPath, fileName)
    df.to_csv(filePath, header=True, index=False)
today = dt.now().date()
print("Data was downloaded {0}".format(today))

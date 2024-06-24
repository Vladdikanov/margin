import datetime
from functools import reduce
from pprint import pprint
import unicodedata

import httplib2
from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

import client_info

creds = client_info.creds

def add_margin_table(data, sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    values = data
    body = {"values": values}
    result = (service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet}!A1:U',
            valueInputOption="RAW",
            body=body,
        )
        .execute()
    )
def get_name_art(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!A2:D").execute()
    art_name = sheet["values"]
    try:
        dict_name_art = reduce(lambda a,b: a|b, list(map(lambda x: {int(x[0]): x[1]}, art_name)))
    except Exception:
        dict_name_art = {}
    return dict_name_art

def get_volume_name_cost_price(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!A2:D").execute()
    name_costPrice_volume = sheet["values"]
    dict_name_costPrice_volume = {}
    for i in name_costPrice_volume:
        dict_name_costPrice_volume[int(i[0])] = {
            "name": i[1],
            "costPrice": int(i[2]),
            "volume": float((i[3].replace(",",".")))
        }
    return dict_name_costPrice_volume

def get_commission(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!B2:C").execute()
    comission = sheet["values"]
    dict_comission = {}
    for i in comission:
        dict_comission[i[0]] = float(i[1].replace(",", "."))
    return dict_comission



def create_new_margin_table(name_sheets, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    body = {
        "requests": {
            "addSheet": {
                "properties": {
                    "title": f"{name_sheets}",
                    "index": 3,
                    'gridProperties': {
                        'frozenRowCount': 1,
                        'frozenColumnCount': 3
                    }
                }
            }
        }
    }

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def get_data_analitic(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!A3:U").execute()
    data = sheet["values"]
    for i in data:
        print(i)
    return data

def get_abc(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!A3:U").execute()
    data_abc = sheet["values"]
    dict_abc = {}
    for i in data_abc:
        try:
            dict_abc[int(i[0])] = i[5]
        except IndexError:
            continue
    pprint(dict_abc)
    return dict_abc

def get_tax_percent(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!J2").execute()
    tax = float(sheet['values'][0][0]) / 100
    return tax
get_tax_percent("Артикулы", "1OPFX2b9A9pWKVehlkOupGjt7DC0ePjS5CNyM-FlQYcM")
def get_key_req(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!A3:U").execute()
    data_key_req = sheet["values"]
    dict_key_req = {}
    for i in data_key_req:
        try:
            dict_key_req[int(i[0])] = i[4]
        except IndexError:
            continue
    return dict_key_req

def get_plan_art(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)
    try:
        sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!B2:S").execute()
        plan = sheet["values"]
        plan_list = []
        plan_dict = {}
        for i in plan:
            if bool(i) is not False and len(i[0]) > 7:
                try:
                    row = [int(i[0]), float(i[-1].replace(",","."))]
                    plan_list.append(row)
                except Exception as ex:
                    print(ex)
                    continue
        for row in plan_list:
            print(row)
        for i in plan_list:
            plan_dict[i[0]] = i[-1]


        return plan_dict
    except Exception as ex:
        print(ex)
        return {}

def get_demand_art(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)
    try:
        sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!B2:Q").execute()
        demand = sheet["values"]
        demand_list = []
        demand_dict = {}
        for i in demand:
            if bool(i) is not False and len(i[0]) > 7 and "." not in i[0]:
                try:
                    row = [int(i[0]), float(unicodedata.normalize("NFKD",i[-1]).replace(" ", "").replace(",", "."))]
                    demand_list.append(row)
                except Exception as ex:
                    print(ex)
                    continue
        for row in demand_list:
            print(row)
        for i in demand_list:
            demand_dict[i[0]] = i[-1]

        return demand_dict
    except Exception as ex:
        return {}

def format_sheets(id_list, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    body = {
        "requests": [
            {
                "repeatCell": {
                    "range":
                        {
                            "sheetId": id_list,
                            "startRowIndex": 0,
                            "endRowIndex": 2,
                            "startColumnIndex": 0,
                            "endColumnIndex": 3
                        },
                    "cell":
                        {
                            "userEnteredFormat":
                                {
                                    "backgroundColor":
                                        {
                                            "red": 182 / 255,
                                            "green": 215 / 255,
                                            "blue": 168 / 255,
                                            "alpha": 1
                                        },
                                    "textFormat":
                                        {
                                            "bold": True
                                        }

                                }
                        },
                    "fields": "userEnteredFormat"
                }
            },
            {
                "repeatCell": {
                    "range":
                        {
                            "sheetId": id_list,
                            "startRowIndex": 0,
                            "endRowIndex": 2,
                            "startColumnIndex": 3,
                            "endColumnIndex": 6
                        },
                    "cell":
                        {
                            "userEnteredFormat":
                                {
                                    "backgroundColor":
                                        {
                                            "red": 204 / 255,
                                            "green": 204 / 255,
                                            "blue": 204 / 255,
                                            "alpha": 1
                                        },
                                    "textFormat":
                                        {
                                            "bold": True
                                        }

                                }
                        },
                    "fields": "userEnteredFormat"
                }
            },
            {
                "repeatCell": {
                    "range":
                        {
                            "sheetId": id_list,
                            "startRowIndex": 0,
                            "endRowIndex": 2,
                            "startColumnIndex": 6,
                            "endColumnIndex": 12
                        },
                    "cell":
                        {
                            "userEnteredFormat":
                                {
                                    "backgroundColor":
                                        {
                                            "red": 252 / 255,
                                            "green": 229 / 255,
                                            "blue": 205 / 255,
                                            "alpha": 1
                                        },
                                    "textFormat":
                                        {
                                            "bold": True
                                        }

                                }
                        },
                    "fields": "userEnteredFormat"
                }
            },
            {
                "repeatCell": {
                    "range":
                        {
                            "sheetId": id_list,
                            "startRowIndex": 0,
                            "endRowIndex": 2,
                            "startColumnIndex": 12,
                            "endColumnIndex": 15
                        },
                    "cell":
                        {
                            "userEnteredFormat":
                                {
                                    "backgroundColor":
                                        {
                                            "red": 201 / 255,
                                            "green": 218 / 255,
                                            "blue": 248 / 255,
                                            "alpha": 1
                                        },
                                    "textFormat":
                                        {
                                            "bold": True
                                        }

                                }
                        },
                    "fields": "userEnteredFormat"
                }
            },
            {
                "repeatCell": {
                    "range":
                        {
                            "sheetId": id_list,
                            "startRowIndex": 0,
                            "endRowIndex": 2,
                            "startColumnIndex": 15,
                            "endColumnIndex": 34
                        },
                    "cell":
                        {
                            "userEnteredFormat":
                                {
                                    "backgroundColor":
                                        {
                                            "red": 182 / 255,
                                            "green": 215 / 255,
                                            "blue": 168 / 255,
                                            "alpha": 1
                                        },
                                    "textFormat":
                                        {
                                            "bold": True
                                        }

                                }
                        },
                    "fields": "userEnteredFormat"
                }
            },
            {
                "mergeCells": {
                    "range": {
                        "sheetId": id_list,
                        "startRowIndex": 1,
                        "endRowIndex": 2,
                        "startColumnIndex": 0,
                        "endColumnIndex": 3
                    },
                    "mergeType": "MERGE_ALL"
                }
            },
            {
                "mergeCells": {
                    "range": {
                        "sheetId": id_list,
                            "startRowIndex": 1,
                            "endRowIndex": 2,
                            "startColumnIndex": 3,
                            "endColumnIndex": 6
                    },
                    "mergeType": "MERGE_ALL"
                }
            },
            {
                "mergeCells": {
                    "range": {
                        "sheetId": id_list,
                            "startRowIndex": 1,
                            "endRowIndex": 2,
                            "startColumnIndex": 6,
                            "endColumnIndex": 12
                    },
                    "mergeType": "MERGE_ALL"
                }
            },
            {
                "mergeCells": {
                    "range": {
                        "sheetId": id_list,
                        "startRowIndex": 1,
                        "endRowIndex": 2,
                        "startColumnIndex": 12,
                        "endColumnIndex": 15
                    },
                    "mergeType": "MERGE_ALL"
                }
            },
            {
                "mergeCells": {
                    "range": {
                        "sheetId": id_list,
                        "startRowIndex": 1,
                        "endRowIndex": 2,
                        "startColumnIndex": 15,
                        "endColumnIndex": 34
                    },
                    "mergeType": "MERGE_ALL"
                }
            },
            {
                "repeatCell": {
                    "range":
                        {
                            "sheetId": id_list,
                            "startRowIndex": 1,
                            "endRowIndex": 2,
                            "startColumnIndex": 0,
                            "endColumnIndex": 34
                        },
                    "cell":
                        {
                            "userEnteredFormat":
                                {
                                    "horizontalAlignment": "CENTER"

                                }
                        },
                    "fields": "userEnteredFormat(horizontalAlignment)"
                }
            },


        ]
    }
    print("Применяю")

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def get_id_sheet(sheet, sheet_id):
    CREDENTIALS_FILE = creds
    spreadsheet_id = sheet_id
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/spreadsheets'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = discovery.build("sheets", 'v4', http=httpAuth)

    sheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id, ranges=[sheet]).execute()
    id_sheet = sheet['sheets'][0]['properties']['sheetId']
    return id_sheet

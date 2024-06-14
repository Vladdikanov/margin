import json
import time
import datetime

import requests
from pprint import pprint

# Заголовки
def get_header(api_key):
    # with open("key_wb_scarf.json", "r") as file:
    #     key = json.load(file)
    # token = key["key"]
    token = api_key

    headers = {
        "Authorization": token,
        "Content type": "application/json"

    }
    return headers
# Список рекламных компаний
def get_companys(headers):
    complist = []
    list_company = requests.get("https://advert-api.wb.ru/adv/v1/promotion/count", headers=headers).json()
    print(list_company)
    for i in list_company["adverts"]:
        if i["status"] in [9, 11]:
            for id in i['advert_list']:
                complist.append(id['advertId'])
    print(complist)
    return complist

# Затраты на продвижение артикула
def get_marketing(headers, date_param):
    complist = get_companys(headers)
    dictstatSum = {}
    dictstatCTR = {}
    for comp in complist:
        print(comp)
        while True:
            params = [{"id": comp,
                        "dates": [date_param]}]
            result = requests.post("https://advert-api.wb.ru/adv/v2/fullstats", headers=headers, json=params)

            if result.status_code == 429:
                print(result.status_code)
                time.sleep(60)
                continue
            elif result.status_code == 502:
                print(result.status_code)
                print(result)
                time.sleep(5)

            elif result.status_code == 400:
                print(result.status_code)
                res = None
                break
            elif result.status_code == 200:
                print(result.status_code)
                res = result.json()
                pprint(res)
                break
            else:
                print(result.status_code)
        if res != None:
            for i in res[0]['days'][0]['apps']:
                for nm in i['nm']:
                    if nm['nmId'] not in dictstatSum:
                        dictstatSum[nm['nmId']] = nm["sum"]
                    else:
                        dictstatSum[nm['nmId']] += nm["sum"]
                        print("Суммировали", nm['nmId'], nm["sum"])
            for i in res[0]['days'][0]['apps']:
                for nm in i['nm']:
                    if nm['nmId'] not in dictstatCTR:
                        dictstatCTR[nm['nmId']] = {
                            "views": nm['views'],
                            'clicks': nm['clicks']

                        }
                    else:
                        dictstatCTR[nm['nmId']]['views'] += nm['views']
                        dictstatCTR[nm['nmId']]['clicks'] += nm['clicks']

    pprint(dictstatCTR)
    for id in dictstatCTR:
        views = dictstatCTR[id]['views']
        clicks = dictstatCTR[id]['clicks']
        if views != 0:
            ctr = (clicks / views) * 100
            dictstatCTR[id] = round(ctr, 2)
        else:
            dictstatCTR[id] = 0

    pprint(dictstatCTR)
    pprint(dictstatSum)
    return dictstatSum, dictstatCTR



# Кэф складов
def get_tariffs_box(headers, date):
    params = {
        "date": f"{date}"
    }
    tarifs_dict = {}
    headers = headers
    res_box = requests.get("https://common-api.wildberries.ru/api/v1/tariffs/box", headers=headers, params=params).json()
    tarifs_box = res_box['response']["data"]['warehouseList']
    for wh in tarifs_box:
        tarifs_dict[wh['warehouseName']] = {
            "DeliveryBase": wh['boxDeliveryBase'],
            "DeliveryLiter": wh['boxDeliveryLiter'],
            "StorageBase": wh['boxStorageBase'],
            "StorageLiter": wh['boxStorageLiter']
        }
    return tarifs_dict




# Аналитика по карточкам товара
def get_card_analytics(headers, date_1ago, date_30ago):
    params = {
        "period": {
            "begin": f"{date_30ago} 00:00:00",
            "end": f"{date_1ago} 23:59:59"
        },
        "orderBy": {
            "field": "orders",
            "mode": "asc"
        },
        "page": 1
    }
    card_analytics_dict = {}
    res = requests.post("https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail", headers=headers, json=params).json()
    pprint(res)
    stistic = res['data']['cards']
    next_page = res["data"]['isNextPage'] # Если есть следующая страница, стоит пополнить словарь данными
    for card in stistic:
        card_id = card['nmID']
        category = card['object']['name']
        buyoutsPercent = card['statistics']['selectedPeriod']['conversions']['buyoutsPercent']
        card_analytics_dict[card_id] = {
            "category": category,
            "buyoutsPercent": buyoutsPercent
        }
    return card_analytics_dict

# Средняя цена
def get_avg_price(headers, date_1ago):

    params = {
        "period": {
            "begin": f"{date_1ago} 00:00:00",
            "end": f"{date_1ago} 23:59:59"
        },
        "orderBy": {
            "field": "orders",
            "mode": "asc"
        },
        "page": 1
    }
    avg_price_dict = {}
    res = requests.post("https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail", headers=headers, json=params).json()
    stistic = res['data']['cards']
    next_page = res["data"]['isNextPage'] # Если есть следующая страница, стоит пополнить словарь данными
    for card in stistic:
        card_id = card['nmID']
        avgPriceRub = card['statistics']['selectedPeriod']['avgPriceRub']
        avg_price_dict[card_id] = avgPriceRub
    return avg_price_dict
# Заказы
def get_orders(headers, date_1ago):
    params = {
        "dateFrom": f'{date_1ago}',
        "flag": 1
    }
    while True:
        orders_res = requests.get("https://statistics-api.wildberries.ru/api/v1/supplier/orders", headers=headers,
                                      params=params)
        if orders_res.status_code == 200:
            orders = orders_res.json()
            break
        elif orders_res.status_code == 429:
            time.sleep(60)
            continue
    return orders

# Остатки
def get_stocks(headers, date_1ago):
    params = {
        "dateFrom": f'{date_1ago}',
        "flag": 1
    }
    while True:
        stocks_res = requests.get("https://statistics-api.wildberries.ru/api/v1/supplier/stocks", headers=headers,
                                  params=params)
        if stocks_res.status_code == 200:
            stocks = stocks_res.json()
            break
        elif stocks_res.status_code == 429:
            time.sleep(60)
            continue
    return stocks

# Для таблицы продажи
def get_sales_orders_stock(headers, date_1ago):
    print("Расчет")
    params = {
        "dateFrom": f'{date_1ago}',
        "flag": 1
    }
    # Получаем продажи по апи попродажам
    while True:
        sales_res = requests.get("https://statistics-api.wildberries.ru/api/v1/supplier/sales", headers=headers,
                                      params=params)
        if sales_res.status_code == 200:
            sales = sales_res.json()
            break
        elif sales_res.status_code == 429:
            time.sleep(60)
            continue
    # Получаем заказы по апи
    while True:
        orders_res = requests.get("https://statistics-api.wildberries.ru/api/v1/supplier/orders", headers=headers,
                                      params=params)
        if orders_res.status_code == 200:
            orders = orders_res.json()
            break
        elif orders_res.status_code == 429:
            time.sleep(60)
            continue
        # Получаем остатки по апи
    while True:
        stocks_res = requests.get("https://statistics-api.wildberries.ru/api/v1/supplier/stocks", headers=headers,
                                  params=params)
        if stocks_res.status_code == 200:
            stocks = stocks_res.json()
            break
        elif stocks_res.status_code == 429:
            time.sleep(60)
            continue
    # Формируем словарь с данными о продажах, заказах и остатках
    sales_orders_stocks = {}
    all_sales = 0
    for card in sales:
        if sales_orders_stocks.get(card['nmId']) == None:
            sales_orders_stocks[card['nmId']] = {"sales_orders_stocks":{card['warehouseName']:{'sales': 1}},
                                                 "info":{'subject': card['subject']}}
            all_sales += 1

        elif sales_orders_stocks[card['nmId']]["sales_orders_stocks"].get(card['warehouseName']) == None:
            sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']] = {'sales': 1}

            all_sales += 1

        else:
            sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']]['sales'] += 1

            all_sales += 1

    for card in orders:
        if sales_orders_stocks.get(card['nmId']) == None:
            sales_orders_stocks[card['nmId']] = {"sales_orders_stocks":{card['warehouseName']:{'orders': 1}},
                                                 "info":{'subject': card['subject']}}

        elif sales_orders_stocks[card['nmId']]["sales_orders_stocks"].get(card['warehouseName']) == None:
            sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']] = {'orders': 1}
        elif sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']].get('orders') == None:
            sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']]['orders'] = 1
        else:
            sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']]['orders'] += 1

    for card in stocks:
        if card["quantity"] == 0:
            continue
        if sales_orders_stocks.get(card['nmId']) == None:
            sales_orders_stocks[card['nmId']] = {"sales_orders_stocks":{card['warehouseName']:{'stocks': card['quantity']}},
                                                 "info":{'subject': card['subject']}}

        elif sales_orders_stocks[card['nmId']]["sales_orders_stocks"].get(card['warehouseName']) == None:
            sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']] = {'stocks': card['quantity']}
        elif sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']].get('stocks') == None:
            sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']]['stocks'] = card['quantity']
        else:
            sales_orders_stocks[card['nmId']]["sales_orders_stocks"][card['warehouseName']]['stocks'] += card['quantity']
    # Итог по общим остаткам, продажам и заказам

    for id in sales_orders_stocks:
        all_sales = 0
        all_orders = 0
        all_stocks = 0
        for wh in sales_orders_stocks[id]["sales_orders_stocks"].values():
            all_sales += wh.get("sales", 0)
            all_orders += wh.get("orders", 0)
            all_stocks += wh.get("stocks", 0)
        sales_orders_stocks[id]["info"]['all_orders'] = all_orders
        sales_orders_stocks[id]["info"]['all_sales'] = all_sales
        sales_orders_stocks[id]["info"]['all_stocks'] = all_stocks

    pprint(sales_orders_stocks)
    return sales_orders_stocks

def get_report_on_sales(headers, dateFrom, dateTo):
    params = {
        "dateFrom": f'{dateFrom}',
        "dateTo": f"{dateTo}",
    }
    while True:
        report_res = requests.get("https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod", headers=headers,
                                  params=params)
        if report_res.status_code == 200:
            report = report_res.json()
            break
        elif report_res.status_code == 429:
            time.sleep(60)
            continue
    pprint(report)
    print(len(report))
    return report

def get_orders_task(headers):

    while True:
        orders_task_res = requests.get("https://suppliers-api.wildberries.ru/api/v3/orders/new",
                                  headers=headers)
        if orders_task_res.status_code == 200:
            task = orders_task_res.json().get("orders", None)
            break
        elif orders_task_res.status_code == 429:
            time.sleep(60)
            continue
    pprint(task)
    return task





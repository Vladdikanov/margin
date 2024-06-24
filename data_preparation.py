import math
from pprint import pprint


def add_volume_name_cost_price(main_data, volume_name_cost_price):
    for id in main_data:
        if volume_name_cost_price.get(id) == None:
            main_data[id]["info"]["volume"] = 0
            main_data[id]["info"]['costPrice'] = 0
            main_data[id]["info"]['name'] = 0
        else:
            main_data[id]["info"]["volume"] = volume_name_cost_price[id]["volume"]
            main_data[id]["info"]['costPrice'] = volume_name_cost_price[id]['costPrice']
            main_data[id]["info"]['name'] = volume_name_cost_price[id]['name']
    pprint(main_data)
    return main_data

def add_tax(main_data, tax_percent):
    for id in main_data:
        main_data[id]["info"]["tax"] = main_data[id]["info"]['avgPrice'] * tax_percent

def add_abc(main_data, abc):
    pprint(abc)
    for id in main_data:
        main_data[id]["info"]["abc"] = abc.get(id, None)

def add_keyReq(main_data, keyReq):
    pprint(keyReq)
    for id in main_data:
        main_data[id]["info"]["key_req"] = keyReq.get(id, None)

def add_storage(tarifs, info):
    for id in info:
        print(id)
        vol = info[id]["info"]["volume"]
        print(vol)
        if vol == 0:
            volume = 0
        elif vol <= 1:
            volume = 1
        else:
            volume = math.ceil(vol)
        storageForAllWH = 0
        for wh, s_o_s in info[id]['sales_orders_stocks'].items():
            if wh != "Минск":
                if wh not in tarifs:
                    print("Пропускаем ", wh)
                    continue
                StorageBase = float(tarifs[wh]['StorageBase'].replace(",", "."))
                StorageLiter = float(tarifs[wh]['StorageLiter'].replace(",", "."))
            else:
                StorageBase = 0.16
                StorageLiter = 0.085

            if volume == 1:
                storageForOne = StorageBase
                info[id]['sales_orders_stocks'][wh]["storageForOne"] = storageForOne
                storageForAll = storageForOne * info[id]['sales_orders_stocks'][wh].get("stocks", 0)
                info[id]['sales_orders_stocks'][wh]["storageForAll"] = storageForAll
                storageForAllWH += storageForAll
            else:
                storageForOne = StorageBase + (StorageLiter * (volume - 1))
                info[id]['sales_orders_stocks'][wh]["storageForOne"] = storageForOne
                storageForAll = storageForOne * info[id]['sales_orders_stocks'][wh].get("stocks", 0)
                info[id]['sales_orders_stocks'][wh]["storageForAll"] = storageForAll
                storageForAllWH += storageForAll
        info[id]['info']['storageForAllWH'] = storageForAllWH
        all_orders = info[id]['info'].get('all_orders', 0)
        if all_orders != 0:
            info[id]['info']['storageForOneOrder'] = storageForAllWH / all_orders
        else:
            info[id]['info']['storageForOneOrder'] = 0

    pprint(info)
def add_avgPrice(main_info, avgPrice_dict):
    for id in main_info:
        main_info[id]["info"]['avgPrice'] = avgPrice_dict[id]
    pprint(main_info)
def add_comission(main_info, comission_dict):
    for id in main_info:
        sub = main_info[id]["info"]['subject']
        main_info[id]["info"]["comission"] = comission_dict[sub]
    pprint(main_info)
def add_buyoutsPercent(main_info, analitics):
    for id in main_info:
        main_info[id]["info"]["buyoutsPercent"] = analitics[id]["buyoutsPercent"]
    pprint(main_info)
    return main_info

def add_logistics(tarifs, info):
    for id in info:
        vol = info[id]["info"].get("volume", 0)
        bp = info[id]["info"]["buyoutsPercent"]
        if bp == 0:
            buyoutsPercent = 100
        else:
            buyoutsPercent = bp

        if vol == 0:
            volume = 0
        elif vol <= 1:
            volume = 1
        else:
            volume = math.ceil(vol)
        print(id)
        print(volume)
        deliveryForAllWH = 0
        for wh, s_o_s in info[id]['sales_orders_stocks'].items():
            if wh != "Минск":
                if wh not in tarifs:
                    print("Пропускаем ", wh)
                    continue
                DeliveryBase = float(tarifs[wh]['DeliveryBase'].replace(",","."))
                DeliveryLiter = float(tarifs[wh]['DeliveryLiter'].replace(",","."))
            else:
                DeliveryBase = 40.5
                DeliveryLiter = 9.45

            if volume == 1:
                deliveryForOne = DeliveryBase
                info[id]['sales_orders_stocks'][wh]["deliveryForOne"] = deliveryForOne
                orders = info[id]['sales_orders_stocks'][wh].get("orders", 0)
                deliveryForAll = deliveryForOne * orders
                info[id]['sales_orders_stocks'][wh]["deliveryForAll"] = deliveryForAll
                deliveryForAllWH += deliveryForAll
            else:
                deliveryForOne = DeliveryBase + (DeliveryLiter * (volume - 1))
                info[id]['sales_orders_stocks'][wh]["deliveryForOne"] = deliveryForOne
                orders = info[id]['sales_orders_stocks'][wh].get("orders", 0)
                deliveryForAll = deliveryForOne * orders
                info[id]['sales_orders_stocks'][wh]["deliveryForAll"] = deliveryForAll
                deliveryForAllWH += deliveryForAll
        info[id]['info']['deliveryForAllWH'] = deliveryForAllWH
        all_orders = info[id]['info'].get('all_orders', 0)
        if all_orders != 0:
            print(deliveryForAllWH)
            deliveryForAllWH_back = all_orders * ((100 - buyoutsPercent) / 100) * 50
            print(deliveryForAllWH_back)
            print(all_orders)
            print(buyoutsPercent)
            deliveryForOneOrder = (deliveryForAllWH + deliveryForAllWH_back) / (all_orders * (buyoutsPercent / 100))
            info[id]['info']['deliveryForOneOrder'] = deliveryForOneOrder
        else:
            info[id]['info']['deliveryForOneOrder'] = 0
    pprint(info)
def add_marceting(main_data, marketing_dict):
    for id in marketing_dict:
        marketing = marketing_dict[id]
        # Исправить баг, затраты на маркетинг есть по товару, но среди продаж, хранения и т.д. его нет
        try:
            orders = main_data[id]["info"]['all_orders']
        except Exception:
            continue
        buyoutsPercent = main_data[id]["info"]['buyoutsPercent']
        main_data[id]["info"]["marketing"] = marketing
        if orders != 0:
            marketingForOneOrder = marketing/(orders * (buyoutsPercent / 100))
            main_data[id]["info"]["marketingForOneOrder"] = marketingForOneOrder
        else:
            marketingForOneOrder = 0
            main_data[id]["info"]["marketingForOneOrder"] = marketingForOneOrder

def add_ctr(main_data, ctr_dict):
    # for id in ctr_dict:
    #     main_data[id]["info"]["ctr"] = ctr_dict[id]

    for id in main_data:
        main_data[id]["info"]["ctr"] = ctr_dict.get(id, 0)

def add_plan(main_data, plan_dict):

    for id in main_data:
        main_data[id]["info"]["plan"] = plan_dict.get(id, 0)

def add_demand(main_data, demand_dict):

    for id in main_data:
        main_data[id]["info"]["demand"] = demand_dict.get(id, 0)

def add_profit(main_data):
    for id in main_data:
        orders = main_data[id]["info"]['all_orders']
        if orders != 0:
            avgPrice = main_data[id]["info"]['avgPrice']
            buyoutsPercent = main_data[id]["info"]['buyoutsPercent']
            storageForOneOrder = main_data[id]["info"]['storageForOneOrder']
            deliveryForOneOrder = main_data[id]["info"]['deliveryForOneOrder']
            costPrice = main_data[id]["info"]['costPrice']
            comission = main_data[id]["info"]['comission']
            tax = main_data[id]["info"]['tax']
            marketingForOneOrder = main_data[id]["info"].get('marketingForOneOrder', 0)
            profitForOneOrder = avgPrice - storageForOneOrder - deliveryForOneOrder - costPrice - (avgPrice * (comission / 100)) - tax -marketingForOneOrder
            main_data[id]["info"]['profitForOneOrder'] = profitForOneOrder
            profit = main_data[id]["info"]['profitForOneOrder'] * orders * (buyoutsPercent / 100)
        else:
            marketingForOneOrder = main_data[id]["info"].get('marketingForOneOrder', 0)
            marketing = main_data[id]["info"].get('marketing', 0)
            profitForOneOrder = 0 - marketingForOneOrder
            # buyoutsPercent = main_data[id]["info"]['buyoutsPercent']
            main_data[id]["info"]['profitForOneOrder'] = profitForOneOrder
            profit = 0 - marketing
        # profit = main_data[id]["info"]['profitForOneOrder'] * orders * (buyoutsPercent / 100)
        main_data[id]["info"]['profit'] = profit
def add_margin(main_data):
    for id in main_data:
        avgPrice = main_data[id]['info']['avgPrice']
        profitForOneOrder = main_data[id]['info']['profitForOneOrder']
        if avgPrice != 0:
            margin = profitForOneOrder / avgPrice * 100
            main_data[id]['info']['margin'] = margin

        else:
            margin = 0
            main_data[id]['info']['margin'] = margin

def add_percentage_of_investments(main_data):
    for id in main_data:
        costPrice = main_data[id]['info'].get('costPrice', 0)
        profitForOneOrder = main_data[id]['info']['profitForOneOrder']
        print(costPrice)
        print(profitForOneOrder)
        print(id )
        if profitForOneOrder != 0:
            if costPrice != 0:
                percent_of_investments = profitForOneOrder / costPrice * 100
                main_data[id]['info']['percent_of_investments'] = percent_of_investments
            else:
                main_data[id]['info']['percent_of_investments'] = percent_of_investments

        else:
            percent_of_investments = 0
            main_data[id]['info']['percent_of_investments'] = percent_of_investments

def add_orders_conversion(main_data):
    for id in main_data:
        orders = main_data[id]['info']['all_orders']
        buyoutsPercent = main_data[id]['info']['buyoutsPercent']
        main_data[id]['info']['orders_conversion'] = orders * (buyoutsPercent / 100)
def add_sales_volume(main_data):
    for id in main_data:
        avgPrice = main_data[id]['info']['avgPrice']
        orders_conversion = main_data[id]['info']['orders_conversion']
        main_data[id]['info']['sales_volume'] = orders_conversion * avgPrice

def create_data_list_to_db(main_data):
    data_list = []

    for id in main_data:
        name = main_data[id]["info"]["name"]
        orders = main_data[id]["info"]["all_orders"]
        sales = main_data[id]["info"]["all_sales"]
        avgPrice = main_data[id]["info"]["avgPrice"]
        comission = main_data[id]["info"]["comission"]
        volume = main_data[id]["info"]["volume"]
        storageForOneOrder = round(main_data[id]["info"]["storageForOneOrder"], 2)
        storageForAllWH = round(main_data[id]["info"]["storageForAllWH"], 2)
        buyoutsPercent = main_data[id]["info"]["buyoutsPercent"]
        deliveryForOneOrder = round(main_data[id]["info"]["deliveryForOneOrder"], 2)
        costPrice = main_data[id]["info"]["costPrice"]
        tax = round(main_data[id]["info"]["tax"], 2)
        profitForOneOrder = round(main_data[id]["info"]["profitForOneOrder"], 2)
        margin = round(main_data[id]["info"]["margin"], 2)
        percent_of_investments = round(main_data[id]["info"]["percent_of_investments"], 2)
        orders_conversion = round(main_data[id]["info"]["orders_conversion"], 2)
        sales_volume = round(main_data[id]["info"]["sales_volume"], 2)
        marketing = round(main_data[id]["info"].get("marketing", 0), 2)
        marketingForOneOrder = round(main_data[id]["info"].get("marketingForOneOrder", 0), 2)
        profit = round(main_data[id]["info"]["profit"], 2)
        ctr = round(main_data[id]["info"].get("ctr", 0), 2)
        abc = main_data[id]["info"]["abc"]
        plan = main_data[id]['info'].get("plan", 0)
        demand = main_data[id]["info"].get("demand", 0)
        stocks = main_data[id]["info"].get("all_stocks", 0)


        row = [name, id, orders, sales, avgPrice, comission, volume, storageForOneOrder, storageForAllWH, buyoutsPercent,
               deliveryForOneOrder, costPrice, tax, profitForOneOrder, margin, percent_of_investments, orders_conversion,
               sales_volume, marketing, marketingForOneOrder, profit, ctr, abc, plan, demand, stocks]

        data_list.append(row)

    return data_list
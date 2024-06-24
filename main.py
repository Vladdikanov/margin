import asyncio

from pprint import pprint
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging
import datetime

import client_info
from data_preparation import add_sales_volume, create_data_list_to_db, add_orders_conversion, add_abc, add_tax,\
    add_ctr, add_plan, add_profit, add_storage, add_margin, add_comission, add_marceting, add_demand, add_logistics,\
    add_buyoutsPercent, add_avgPrice, add_volume_name_cost_price, add_percentage_of_investments

from sheets_wbseller_margin import get_volume_name_cost_price, get_commission, add_margin_table, create_new_margin_table,\
    get_abc, get_key_req, get_plan_art, get_demand_art, format_sheets, get_id_sheet, get_tax_percent
from API_WB import get_header, get_tariffs_box, get_sales_orders_stock, get_card_analytics, get_avg_price, get_marketing
from global_analitic_db import load_data_to_db, count_and_load, get_seller_and_keys



def start_marginality():

    date_1days_ago = datetime.date.today() - datetime.timedelta(days=1)
    date_30days_ago = datetime.date.today() - datetime.timedelta(days=30 )
    #
    date_now = datetime.date.today().strftime("%Y-%m-%d")
    date_param_1ago = date_1days_ago.strftime("%Y-%m-%d")
    date_param_30ago = date_30days_ago.strftime("%Y-%m-%d")
    shop_keys = get_seller_and_keys(client_info.id_client)
    for seller, api_wb, sheet_id in shop_keys:

        dict_name_costPrice_volume = get_volume_name_cost_price("Артикулы", sheet_id)
        tarifs = get_tariffs_box(get_header(api_wb), date_param_1ago)
        comission = get_commission("Комиссия", sheet_id)
        analitics = get_card_analytics(get_header(api_wb),date_param_1ago, date_param_30ago)
        avgPrice_dict = get_avg_price(get_header(api_wb),date_param_1ago)
        abc_dict = get_abc("Артикулы", sheet_id)
        plan_dict = get_plan_art("План продаж", sheet_id)
        demand_dict = get_demand_art("План продаж", sheet_id)
        tax_percent = get_tax_percent("Артикулы", sheet_id)
        main_data = get_sales_orders_stock(get_header(api_wb),date_param_1ago)
        add_volume_name_cost_price(main_data, dict_name_costPrice_volume)
        add_storage(tarifs, main_data)
        add_buyoutsPercent(main_data, analitics)
        add_logistics(tarifs, main_data)
        add_comission(main_data, comission)
        add_avgPrice(main_data, avgPrice_dict)
        add_tax(main_data, tax_percent)
        add_abc(main_data, abc_dict)
        pprint(main_data)
        marketing, ctr_dict = get_marketing(get_header(api_wb),date_param_1ago)
        pprint(main_data)
        add_marceting(main_data, marketing)
        add_ctr(main_data, ctr_dict)
        add_plan(main_data, plan_dict)
        add_demand(main_data, demand_dict)
        add_profit(main_data)
        add_margin(main_data)
        add_percentage_of_investments(main_data)
        add_orders_conversion(main_data)
        add_sales_volume(main_data)
        data_margin_to_db = create_data_list_to_db(main_data)
        load_data_to_db(data_margin_to_db, seller, date_now)

        count_and_load(seller, date_now, sheet_id)
        list_gs_id = get_id_sheet(date_now, sheet_id)
        print(list_gs_id)
        format_sheets(list_gs_id, sheet_id)


    # pprint(main_data)



async def main():
    print('Запуск')
    logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w")
    logging.info("Запуск программы")
    start_marginality()
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(start_marginality, 'cron', hour=8, minute=0)
    scheduler.start()
    logging.info("Задачи добавлены и запущены")
    while True:
        await asyncio.sleep(5)
asyncio.run(main())

# Изменения произошли на ура



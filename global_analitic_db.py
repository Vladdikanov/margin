from pprint import pprint

import psycopg2
import json
from config_db import host, user, password, database
from sheets_wbseller_margin import get_data_analitic, create_new_margin_table, add_margin_table


# with open("key_wb_scarf.json", "r") as file:
#     key = json.load(file)['key']
#     print(key)

def connection():
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database,
    )
    return conn


def load_data_to_db_from_gs(data, seller, date):
    for i in data:
        seller = seller
        name_art = i[0]
        id_art = int(i[1])
        orders = int(i[2])
        sales = int(i[3])
        avgPrice = int(i[4])
        comission = int(i[5].replace("%", ""))
        value = float(i[6].replace(",", "."))
        storage_one = float(i[7].replace(",", "."))
        storage_all = float(i[8].replace(",", "."))
        buyoutsPercent = int(i[9].replace("%", ""))
        logistics = float(i[10].replace(",", "."))
        cost_price = int(i[11])
        tax = float(i[12].replace(",", "."))
        profit_one = float(i[13].replace(",", "."))
        margin = float(i[14].replace("%", "").replace(",","."))
        percent_invest = float(i[15].replace("%", "").replace(",","."))
        orders_conversion = float(i[16].replace(",", "."))
        sales_volume = float(i[17].replace(",", "."))
        marketing_all = float(i[18].replace(",", "."))
        marketing_one = float(i[19].replace(",", "."))
        profit_all = float(i[20].replace(",", "."))

        date = date
        while True:
            try:
                pass
                conn = connection()
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO analytics(name_apis, name_art, id_art, orders, sales, avg_price, comission,
                    value, storage_one, storage_all, buyoutsPercent, logistics, cost_price, tax, profit_one, margin, 
                    percent_invest, orders_conversion, sales_volume, marketing_all, marketing_one, profit_all, date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s)""", (seller, name_art, id_art, orders, sales, avgPrice, comission, value, storage_one,
                                 storage_all, buyoutsPercent, logistics, cost_price, tax, profit_one, margin,
                                 percent_invest,
                                 orders_conversion, sales_volume, marketing_all, marketing_one, profit_all, date))
                    conn.commit()
                    conn.close()
                    print(f"Записали {id_art}")
                    print("Соединение закрыто")
                    break

            except Exception as ex:
                print(f"Ошибка подключения: {ex}")
                continue

def load_data_to_db(data, seller, date):
    for i in data:
        seller = seller
        name_art = i[0]
        id_art = int(i[1])
        orders = int(i[2])
        sales = int(i[3])
        avgPrice = int(i[4])
        comission = float(i[5])
        value = float(i[6])
        storage_one = float(i[7])
        storage_all = float(i[8])
        buyoutsPercent = int(i[9])
        logistics = float(i[10])
        cost_price = int(i[11])
        tax = float(i[12])
        profit_one = float(i[13])
        margin = float(i[14])
        percent_invest = float(i[15])
        orders_conversion = float(i[16])
        sales_volume = float(i[17])
        marketing_all = float(i[18])
        marketing_one = float(i[19])
        profit_all = float(i[20])
        ctr = float(i[21])
        abc = i[22]
        plan = i[23]
        demand = i[24]
        stocks = i[25]


        date = date
        while True:
            try:
                pass
                conn = connection()
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO analytics(shop_id, name_art, id_art, orders, sales, avg_price, comission,
                    value, storage_one, storage_all, buyoutsPercent, logistics, cost_price, tax, profit_one, margin, 
                    percent_invest, orders_conversion, sales_volume, marketing_all, marketing_one, profit_all, ctr,
                    abc, plan_day, demand, date, stocks
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s)""", (seller, name_art, id_art, orders, sales, avgPrice, comission, value, storage_one,
                                         storage_all, buyoutsPercent, logistics, cost_price, tax, profit_one, margin, percent_invest,
                                         orders_conversion, sales_volume, marketing_all, marketing_one, profit_all, ctr, abc, plan, demand, date, stocks))
                    conn.commit()
                    conn.close()
                    print(f"Записали {id_art}")
                    print("Соединение закрыто")
                    break

            except Exception as ex:
                print(f"Ошибка подключения: {ex}")
                continue

def count_avg_metrics(seller):

    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""with avg_tab as (
with avg_table_now as(
select id_art, name_art,
round(avg(orders) over (partition by name_art)) as mid_orders,
round(avg(profit_all) over (partition by name_art)) as mid_profit,
round(avg(margin) over (partition by name_art)) as mid_margin,date
from analytics a
where date >= current_date - interval '6 days' and shop_id = %s
order by date desc, mid_orders desc
)
select * from avg_table_now where date = current_date
)
update analytics
set avg7_orders = (select mid_orders from avg_tab where avg_tab.id_art = analytics.id_art),
avg7_profit_all = (select mid_profit from avg_tab where avg_tab.id_art = analytics.id_art),
avg7_margin = (select mid_margin from avg_tab where avg_tab.id_art = analytics.id_art)
where date = current_date and shop_id = %s;""", (seller, seller))
        conn.commit()
        conn.close()
        print(f"Метрики средних значений посчитаны")

def get_sum_marketing_to_week(seller, dateFrom, dateTo):

    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""select id_art, round(sum(marketing_all)) as marketing from analytics
where date >= %s and date <= %s and shop_id = %s
group by name_art, id_art;
;""", (dateFrom, dateTo, seller))
        sum_marketing = cur.fetchall()
        conn.close()
    dict_sum_marketing = {}
    for row in sum_marketing:
        dict_sum_marketing[row[0]] = row[1]
    pprint(dict_sum_marketing)
    return dict_sum_marketing



def count_dif_metrics(seller):

    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""with def_tab as(
with def_table as(
select id_art, name_art, orders,
lag(orders) over(partition by name_art order by date) as last_orders,
profit_one,
lag(profit_one) over(partition by name_art order by date) as last_profit_one,
margin,
lag(margin) over(partition by name_art order by date) as last_margin,date
from analytics a 
where date >= current_date - interval '1 days' and shop_id = %s
)
select id_art, name_art, orders, last_orders, orders - last_orders as def_ord, profit_one, last_profit_one, profit_one - last_profit_one as def_profit, margin, last_margin, margin - last_margin as def_margin, date from def_table
where date >= current_date
)
update analytics
set dif1_profit_one = (select def_profit from def_tab where def_tab.id_art = analytics.id_art),
dif1_margin = (select def_margin from def_tab where def_tab.id_art = analytics.id_art),
dif1_orders = (select def_ord from def_tab where def_tab.id_art = analytics.id_art)
where date = current_date and shop_id = %s;""", (seller, seller))
        conn.commit()
        conn.close()
        print(f"Метрики разницы значений посчитаны")

def count_drr(seller):

    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""with drr_count_table as(
select id_art, marketing_all / sales_volume as drr from analytics
where date = current_date and orders > 0 and marketing_all > 0 and buyoutspercent > 0 and avg_price > 0 and shop_id = %s
)
update analytics
set drr = (select drr from drr_count_table where drr_count_table.id_art = analytics.id_art)
where date = current_date and shop_id = %s;""", (seller, seller))
        conn.commit()
        conn.close()
        print(f"DRR посчитан")
def get_sum_metrics(seller):
    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""select sum(orders) as orders,
        sum(sales) as sales,
        sum(storage_one) as storage_one,
        sum(storage_all) as storage_all,
        sum(logistics) as logistics,
        sum(profit_one) as profit_one,
        sum(sales_volume) as sales_volume,
        sum(marketing_all) as marketing_all,
        sum(marketing_one) as marketing_one,
        sum(profit_all) as profit_all,
        date from analytics
        where shop_id = %s
        group by date
        having date = current_date;""", (seller,))
        sum_metrics = cur.fetchone()
        conn.close()
        pprint(sum_metrics)
    dict_sum_metrics = {
        "sum_orders": round(sum_metrics[0], 2),
        'sum_sales': round(sum_metrics[1], 2),
        "sum_storage_one": round(sum_metrics[2], 2),
        "sum_storage_all": round(sum_metrics[3], 2),
        "logistic": round(sum_metrics[4], 2),
        "profit_one": round(sum_metrics[5], 2),
        "sales_vol": round(sum_metrics[6], 2),
        "marketing_all": round(sum_metrics[7], 2),
        "marketing_one": round(sum_metrics[8], 2),
        "profit_all": round(sum_metrics[9], 2)
    }
    return dict_sum_metrics

def get_data_analytics(seller):
    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""select name_art, id_art, orders, sales, avg_price, comission, value,
storage_one, storage_all, buyoutspercent, logistics, cost_price, tax, profit_one, dif1_profit_one,
margin, dif1_margin, avg7_margin, percent_invest, orders_conversion, sales_volume, abc, marketing_all, marketing_one,
drr, ctr, profit_all, avg7_profit_all, orders, dif1_orders, avg7_orders, plan_day, demand, stocks from analytics
where date = current_date and shop_id = %s
order by profit_all desc;""", (seller,))
        data_analytics = cur.fetchall()
        conn.close()
        data_analytics_list = []
    for i in data_analytics:
        lst = list(i)
        data_analytics_list.append(lst)
    return data_analytics_list

def create_data_list_from_db(sum_metriss, data_analitics):
    data_list = []

    # head = ['Название', 'Артикул WB', "Категория", 'Ключевой запрос', 'Позиция', 'Заказы шт', 'Выкупы шт', 'Комиссия',
    #         'Объем', 'За хранение (шт)', 'Хранение', 'Процент выкупа', 'Логистика', 'Себес', 'Налог',  'Ср-цена',
    #         'Прибыль шт', 'Разница в прибыли к вчерашнему дню', 'Прибыль', 'Средняя по прибыли за 7 дней',
    #         'Заказов конверсия', 'Продажи', 'Средний спрос по нише', 'Реклама', 'Реклама (шт)', 'ДРР', 'CTR', 'Маржа',
    #         'Разница в марже к вчерашнему дню', 'Средняя по марже за 7 дней', 'От вложений', 'Заказы шт',
    #         'Разница в заказах к вчерашнему дню', 'Средняя по заказам за 7 дней', "План на день",]

    head = ['Название', 'Артикул WB', "Категория",
            'Заказы шт', 'Выкупы шт', 'Заказов конверсия',
            'Комиссия', 'Объем', 'За хранение (шт)', 'Хранение', 'Логистика', 'Налог',
            'Средняя по прибыли за 7 дней', 'Средняя по марже за 7 дней', 'Средняя по заказам за 7 дней',
            'Себес', 'Процент выкупа', 'Ср-цена', 'Продажи', 'Средний спрос по нише','Реклама', 'Реклама (шт)',
            'ДРР', 'CTR', 'Маржа', 'Разница в марже к вчерашнему дню', 'Прибыль шт', 'Прибыль',
            'Разница в прибыли к вчерашнему дню', 'От вложений', 'Разница в заказах к вчерашнему дню', 'Заказы шт',
            "План на день", "Остаток"]
    post_head = ["Наименование", "", "",
                 "База", "", "",
                 "ЮнитЭкономические данные", "", "", "", "", "",
                 "История 7 дней", "", "",
                 "Аналитический блок"]
    data_list.append(head)
    data_list.append(post_head)
    rows_data_without_head = []
    for i in data_analitics:
        name_art = i[0]
        id_art = i[1]
        orders = i[2]
        sales = i[3]
        avg_price = i[4]
        comission = i[5]
        value = i[6]
        storage_one = round(i[7], 2)
        storage_all = round(i[18], 2)
        buyoutspercent = i[9]
        logistics = round(i[10], 2)
        cost_price = i[11]
        tax = i[12]
        profit_one = i[13]
        if i[14] != None:
            dif1_profit_one = round(i[14], 2)
        else:
            dif1_profit_one = None
        margin = i[15]
        if i[16] != None:
            dif1_margin = round(i[16], 2)
        else:
            dif1_margin = i[16]
        avg7_margin = i[17]
        percent_invest = i[18]
        orders_conversion = i[19]
        sales_volume = i[20]
        abc = i[21]
        marketing_all = i[22]
        marketing_one = i[23]
        if i[24] != None:
            drr = round(i[24], 2)
        else:
            drr = i[24]
        ctr = i[25]
        profit_all = i[26]
        avg7_profit_all = i[27]
        orders = i[28]
        dif1_orders = i[29]
        avg7_orders = i[30]
        plan_day = i[31]
        print(i[32])
        demand = round(i[32], 0)
        stocks = i[33]


        row = [name_art, id_art, abc,
               orders, sales, orders_conversion,
               f"{comission}%", value, storage_one, storage_all, logistics, tax,
               avg7_profit_all, f"{avg7_margin}%", avg7_orders,
               cost_price, f"{buyoutspercent}%", avg_price, sales_volume, demand, marketing_all, marketing_one, drr, ctr,
               f"{margin}%", f"{dif1_margin}%", profit_one, profit_all, dif1_profit_one, f"{percent_invest}%",
               dif1_orders, orders, plan_day, stocks]

        rows_data_without_head.append(row)


    data_list.extend(rows_data_without_head)
    row_with_all_res = ["", "Общий итог", "",
                        sum_metriss["sum_orders"], sum_metriss["sum_sales"], "",
                        "", "", sum_metriss["sum_storage_one"], sum_metriss["sum_storage_all"], sum_metriss["logistic"], "",
                        "",  "", "",
                        "", "", "", sum_metriss["sales_vol"], "", sum_metriss["marketing_all"], sum_metriss["marketing_one"],
                        "", "", "", "", sum_metriss["profit_one"], sum_metriss["profit_all"]]

    data_list.insert(2, row_with_all_res)
    return data_list
def count_and_load(seller, date, sheet_id):
    count_avg_metrics(seller)
    count_dif_metrics(seller)
    count_drr(seller)
    create_new_margin_table(date, sheet_id)
    add_margin_table(create_data_list_from_db(get_sum_metrics(seller), get_data_analytics(seller)), date, sheet_id)
#
# count_and_load("scarf", "2024-05-15")
# for i in ['2024-04-29']:
#     print(i)
#     load_data_to_db_from_gs(get_data_analitic(i), "scarf", i)

def get_seller_and_keys(client_id):
    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""select id, key_wb, key_gs_margin from shop_keys where client_id = %s;""", (client_id,))
        shop_keys = cur.fetchall()
        conn.close()
    pprint(shop_keys)
    return shop_keys
#v01_t04_rank_process.py

import os
import sys
import requests
from requests.auth import HTTPBasicAuth
import urllib.parse
from collections import defaultdict
import json
import time

URL = os.getenv("CLICKHOUSE_URL")
DB2 = os.getenv("CLICKHOUSE_DATABASE2")
DB3 = os.getenv("CLICKHOUSE_DATABASE3")
USER = os.getenv("CLICKHOUSE_USER")	    
PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")	

if not all([URL, DB2, DB3, USER, PASSWORD]):
    raise Exception("MG: Missing ClickHouse environment variables")

param_batch_id_dttm	= sys.argv[1]

# ==========================================================
# 1️⃣ Получаем данные одним запросом
# ==========================================================

query = f"""
SELECT
    total_order_id ,
    total_packet_art_id ,
    order_total_sum
FROM 
    {DB3}.fct_deficit_packet_order									
WHERE
    batch_id_dttm = '{param_batch_id_dttm}'
FORMAT JSON
"""														

query_encoded = urllib.parse.quote(query)
full_url = f"{URL}/?database={DB3}&query={query_encoded}"

response = requests.post(
    full_url,
    auth=HTTPBasicAuth(USER, PASSWORD),
    headers={"Content-Type": "text/plain"},
    timeout=60,
)

response.raise_for_status()

data = response.json()
rows = data.get("rows", 0)

if rows == 0:
    raise Exception(f"Нет данных в batch_id_dttm={param_batch_id_dttm}")
																 
# ==========================================================
# 2️⃣ Формируем структуру заказов в памяти
# ==========================================================

orders = defaultdict(set)
order_sum = {}

for row in data["data"]:
    
    art_id = row["total_packet_art_id"]
    order_id = row["total_order_id"]
    # Множества - защита от дублей (если в одном заказе - 2 строки с одним и тем же товаром, то в множестве товар будет один раз)
    orders[order_id].add(art_id)
    
    # Если order_total_sum для total_order_id ещё не записан — записываем
    if order_id not in order_sum:
        order_sum[order_id] = float(row["order_total_sum"])

# ==========================================================
# 3️⃣ Жадный алгоритм покрытия
# ==========================================================

removed_products = set()
hit_parade_product = []
order_rank = []
rank = 1

#start_time = time.perf_counter()

while True:

    # (1) Формируем кандидатов — только сочетания товаров реальных заказов
    candidates = set()

    for arts in orders.values():

        # Исключаем уже использованные товары
		# (1) p for p in products if p not in removed_products - это генератор. Он проходит по каждому товару (p - tuple) в заказе и:
		#  - если товар НЕ в removed_products, оставляет его
		#  - если в removed_products — выбрасывает
		# (2) Что такое frozenset? Это: неизменяемое множество (immutable set)
		# Разница:
		# Тип		Можно менять	Можно быть ключом словаря
		# set			Да					Нет
		# frozenset		Нет					Да
		# (3) candidates.add(filtered)
		# А candidates — это set. Чтобы элемент можно было положить в set, он должен быть hashable.
		# Обычный set — не hashable. frozenset — hashable.
		# (4) frozenset({A, C}) - это множество (порядок не важен), без дублей, неизменяемое, пригодное как ключ или элемент множества
		# (5) Почему не tuple(sorted(...)) ? Потому что дальше мы используем: products.issubset(S): subset-проверки удобнее с set
        filtered = frozenset(p for p in arts if p not in removed_products)	# = frozenset(arts - removed_products)
        if filtered:
            candidates.add(filtered)

	# Если обработаны все товары всех заказов, выходим из цикла - хит-парад готов!
    if not candidates:
        break

    # (2) Считаем weight(S)
    scores = {}
    s_orders = defaultdict(list)
    for S in candidates:
        
        if not S:
            continue
            
        sum_sum = 0
        orders_count = 0

        for order_id, arts in orders.items():

            # Исключаем уже удалённые товары
            filtered_order = arts - removed_products

			# filtered_order.issubset(S) - это проверка, что все элементы filtered_order входят в S
			# Не путать filtered_order.issubset(S) с filtered_order in (S). Это разные проверки.
            if filtered_order and filtered_order.issubset(S):
                sum_sum += order_sum[order_id]
                orders_count += 1
                s_orders[S].append( (order_id, order_sum[order_id]) )

        metric = round( sum_sum / len(S), 2) if S else 0

        scores[S] = (sum_sum, metric, orders_count)

    # Находим самую "плотную" комбинацию
	# Что такое scores?
	# После цикла у тебя структура примерно такая:
	# scores = {
    #     (('u1','A'), ('u3','C'))  : (150.0, 50.0),
    #     (('u2','B'),)             : (220.0, 44.0),
    #     (('u4','D'), ('u5','E'))  : (90.0,  45.0)
    # }
	# Если бы ты написал просто max(scores), Python бы сравнивал ключи между собой, то есть сравнивал бы сами множества. Это нам не нужно.
	# key=lambda s: scores[s][1] - Это означает: «При сравнении элементов используй значение словаря»
	# Берёт каждый ключ, Смотрит его значение, Сравнивает по значению, Возвращает ключ с максимальным значением
	# max(scores, key=lambda s: scores[s][1]) - возвращает КЛЮЧ, у которого максимальное значение. Не значение. Именно ключ.
    
    if not scores:
        break

	# (3) Выбираем максимум по metric
    best_S = max(scores, key=lambda s: scores[s][1])

    best_sum, best_metric, best_orders_count = scores[best_S]

    ########################################################################################################################### mg-14.03.2026
    sum_only_for_best = 0				# sum_only_for_best - это сумма сумм (в рублях) по тем заказам, все дефицитные пакеты которых входят в best_S
    orders_count_only_for_best = 0		# orders_count_only_for_best - счетчик заказов, все дефицитные пакеты которых входят в best_S
    for order_id, arts in orders.items():
        # k.issubset(best_S) - это проверка, что все элементы k входят в best_S
        # Не путать k.issubset(best_S) с k in (best_S). Это разные проверки.  
        if arts.issubset(best_S):
            sum_only_for_best += order_sum[order_id]
            orders_count_only_for_best += 1
        metric_only_for_best = round( sum_only_for_best / len(best_S), 2) if best_S else 0
    ########################################################################################################################### mg-14.03.2026

    #print(f"Rank {rank}: {best_S} -> weight={best_sum}, metric={best_metric}")

    # (4) Добавляем товары в хит-парад
    for art in best_S:
        ########################################################################################################################### mg-14.03.2026
        #hit_parade_product.append( (param_batch_id_dttm, rank, art, best_sum, best_metric) )		
        hit_parade_product.append( (param_batch_id_dttm, rank, art, best_sum, best_metric, best_orders_count, sum_only_for_best, metric_only_for_best, orders_count_only_for_best) )
		########################################################################################################################### mg-14.03.2026
        removed_products.add(art)

    for x in s_orders[best_S]:
        order_rank.append( (rank, x[0], x[1]) )
      
    rank += 1
   
# Сортировать список по убыванию 3-го элемента (order_sum) внутри одинакового 1-го элемента (rank)
order_rank.sort(key=lambda x: (x[0], -x[2]))
#order_rank_sorted = sorted(order_rank, key=lambda x: (x[0], -x[2]))
order_rank = [
    (param_batch_id_dttm, row[0], row[1], row[2], i + 1)
    for i, row in enumerate(order_rank)
]

#end_time = time.perf_counter()

#print(f"\nTotal time: {end_time - start_time:.4f} seconds")

# ==========================================================
# 4.1 Запись результата в базу данных
# ==========================================================

tsv_hit_parade_product = "\n".join(
    ########################################################################################################################### mg-14.03.2026
    #f"{item[0]}\t{item[1]}\t{item[2]}\t{item[3]}\t{item[4]}"
    f"{item[0]}\t{item[1]}\t{item[2]}\t{item[3]}\t{item[4]}\t{item[5]}\t{item[6]}\t{item[7]}\t{item[8]}"
	########################################################################################################################### mg-14.03.2026
    for item in hit_parade_product
)

########################################################################################################################### mg-14.03.2026
#query_insert_hit_parade_product = f"INSERT INTO {DB2}.deficit_packet_rank (batch_id_dttm, rank_id, total_packet_art_id, sum_rank, sum_rank_per_packet) FORMAT TSV\n"
query_insert_hit_parade_product = f"INSERT INTO {DB2}.deficit_packet_rank (batch_id_dttm, rank_id, total_packet_art_id, sum_rank, sum_rank_per_packet, orders_count_rank, sum_rank_only_this_set, sum_rank_per_packet_only_this_set, orders_count_only_for_best) FORMAT TSV\n"
########################################################################################################################### mg-14.03.2026
												
if hit_parade_product:
    insert_response_hit_parade_product = requests.post(
        URL,
        params  = {"database": DB2},
        data    = query_insert_hit_parade_product + tsv_hit_parade_product,
        headers = {"Content-Type": "text/plain"},
        auth    = HTTPBasicAuth(USER, PASSWORD),
        timeout = 30,
    )
    # Внутри примерно так (упрощённо). if response.status_code >= 400: raise HTTPError(response). Текст ошибки лежит в response.text
    insert_response_hit_parade_product.raise_for_status() 

# ==========================================================
# 4.2 Запись результата в базу данных
# ==========================================================

tsv_order_rank = "\n".join(
    f"{item[0]}\t{item[1]}\t{item[2]}\t{item[3]}\t{item[4]}"
    for item in order_rank
)

query_insert_order_rank = f"INSERT INTO {DB2}.deficit_order_rank (batch_id_dttm, rank_id, total_order_id, total_sum, sort_rank) FORMAT TSV\n"
												
if order_rank:
    insert_response_order_rank = requests.post(
        URL,
        params  = {"database": DB2},
        data    = query_insert_order_rank + tsv_order_rank,
        headers = {"Content-Type": "text/plain"},
        auth    = HTTPBasicAuth(USER, PASSWORD),
        timeout = 30,
    )
    # Внутри примерно так (упрощённо). if response.status_code >= 400: raise HTTPError(response). Текст ошибки лежит в response.text
    insert_response_order_rank.raise_for_status() 


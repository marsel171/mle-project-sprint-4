import requests
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(
    filename="test.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)

load_dotenv()

# Пример запроса на получение персональных рекомендаций:
recommendations_url = os.environ.get("REC_URL")
headers = {"Content-type": "application/json", "Accept": "text/plain"}
params = {"user_id": 47}
resp = requests.post(recommendations_url, headers=headers, params=params)
if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    logging.info(f"status code: {resp.status_code}")
logging.info(f"Полученные идентификаторы рекомендаций: {recs}")
# В ответе: Полученные идентификаторы рекомендаций: {'recs': [977, 975, 973, 971, 969, 968, 967, 966, 965, 940, 910, 909, 906, 904, 899, 896, 893, 869, 867, 865, 863, 861, 860, 859, 858, 857, 856, 829, 825, 810, 806, 802, 795, 764, 715, 713, 676, 649, 643, 633, 599, 594, 587, 586, 564, 563, 562, 561, 560, 559, 558, 557, 556, 555, 554, 553, 552, 551, 550, 549, 523, 522, 458, 436, 422, 382, 379, 376, 372, 369, 367, 364, 361, 357, 353, 346, 344, 332, 329, 327, 321, 223, 220, 151, 150, 149, 148, 147, 146, 145, 144, 143, 141, 140, 139, 138, 136, 135, 38, 26]}


# Пример запроса на получение ТОП-рекомендаций (холодный пользователь):
recommendations_url = os.environ.get("REC_URL")
headers = {"Content-type": "application/json", "Accept": "text/plain"}
params = {"user_id": 47000000000}
resp = requests.post(recommendations_url, headers=headers, params=params)
if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    logging.info(f"status code: {resp.status_code}")
logging.info(f"Полученные идентификаторы рекомендаций: {recs}")
# В ответе: Полученные идентификаторы рекомендаций: {'recs': [47627256, 51516485, 24692821, 32947997, 55561798,...}


# запрос на загрузку обновленных файлов рекомендаций
load_recommendations_url = os.environ.get("LOAD_RECS_URL")
headers = {"Content-type": "application/json", "Accept": "text/plain"}
params = {"rec_type": "default", "file_path": "top_popular.parquet"}
resp = requests.get(load_recommendations_url, headers=headers, params=params)
logging.info(f"status_code: {resp.status_code}")
# В ответе: 200


# запрос на вывод статистики
get_statistics_url = os.environ.get("GET_STATATISTICS_URL")
resp = requests.get(get_statistics_url)
if resp.status_code == 200:
    logging.info(resp.json())
else:
    logging.info(f"status code: {resp.status_code}")
# В ответе: {'request_personal_count': 1, 'request_default_count': 0}


# запрос на получение событий для пользователя user_id=16 (событий еще нет)
get_user_events_url = os.environ.get("GET_USER_EVENTS_URL")
headers = {"Content-type": "application/json", "Accept": "text/plain"}
params = {"user_id": 16, "k": 10}
resp = requests.post(get_user_events_url, headers=headers, params=params)
if resp.status_code == 200:
    logging.info(resp.json())
else:
    logging.info(f"status code: {resp.status_code}")
# В ответе: {'events': []}


# запрос на добавление событий для пользователя user_id=16
put_user_events_url = os.environ.get("PUT_USER_EVENTS_URL")
headers = {"Content-type": "application/json", "Accept": "text/plain"}
for i in [679169, 630670, 646516, 19152669, 38646012]:
    print(i)
    params = {"user_id": 16, "item_id": i}
    resp = requests.post(put_user_events_url, headers=headers, params=params)
    if resp.status_code == 200:
        logging.info(resp.json())
    else:
        logging.info(f"status code: {resp.status_code}")
# В ответе: # 679169
#             {'result': 'ok'}
#             630670
#             {'result': 'ok'}
#             646516
#             {'result': 'ok'}
#             19152669
#             {'result': 'ok'}
#             38646012
#             {'result': 'ok'}


# снова сделем запрос на получение событий для пользователя user_id=16 и убедимся, что события добавлены
get_user_events_url = os.environ.get("GET_USER_EVENTS_URL")
headers = {"Content-type": "application/json", "Accept": "text/plain"}
params = {"user_id": 16, "k": 10}
resp = requests.post(get_user_events_url, headers=headers, params=params)
if resp.status_code == 200:
    logging.info(resp.json())
else:
    logging.info(f"status code: {resp.status_code}")
# В ответе: {'events': [38646012, 19152669, 646516, 630670, 679169, 679169]}


# проверим, что для пользователя user_id=16 сгенерировались онлайн-рекомендации
get_online_u2i_url = os.environ.get("GET_ONLINE_U2I_URL")
headers = {"Content-type": "application/json", "Accept": "text/plain"}
params = {"user_id": 16, "k": 100, "N": 10}
resp = requests.post(get_online_u2i_url, headers=headers, params=params)
if resp.status_code == 200:
    logging.info(resp.json())
else:
    logging.info(f"status code: {resp.status_code}")
# В ответе: {'recs': [672687, 647040, 654151, 694683, 38646012, 679169,...}
# а в консоли печатаются треки:
# online rec track name:  ['Toxicity']
# online rec artist name:  ['System of A Down']
# online rec track name:  ['Back in Black']
# online rec artist name:  ['AC/DC']
# online rec track name:  ["The Kids Aren't Alright"]
# online rec artist name:  ['The Offspring']
# online rec track name:  ['Highway to Hell']
# online rec artist name:  ['AC/DC']
# и т.д.


# в конце проверим итоговые (замиксованные) рекомендации для пользователя user_id=16
recommendations_url = os.environ.get("REC_URL")
headers = {"Content-type": "application/json", "Accept": "text/plain"}
params = {"user_id": 16}
resp = requests.post(recommendations_url, headers=headers, params=params)
if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    logging.info(f"status code: {resp.status_code}")
logging.info(f"Полученные идентификаторы рекомендаций: {recs}")
# В ответе: Полученные идентификаторы рекомендаций: {'recs': [672687, 2278985, 647040, 3616433, 654151, 630670, 694683, 10270285,
# а в консоли печатаются треки:
# online rec track name:  ['Toxicity']
# online rec artist name:  ['System of A Down']
# online rec track name:  ['Rolling In The Deep']
# online rec artist name:  ['Adele']
# online rec track name:  ['Back in Black']
# online rec artist name:  ['AC/DC']
# online rec track name:  ['Summertime Sadness']
# online rec artist name:  ['Lana Del Rey']
# online rec track name:  ["The Kids Aren't Alright"]
# online rec artist name:  ['The Offspring']
# online rec track name:  ["You're Gonna Go Far, Kid"]
# online rec artist name:  ['The Offspring']
# online rec track name:  ['Highway to Hell']
# online rec artist name:  ['AC/DC']
# online rec track name:  ['Young And Beautiful']
# online rec artist name:  ['Lana Del Rey']
# и т.д.

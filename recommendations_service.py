# FastAPI-микросервис для выдачи рекомендаций, который:

#     принимает запрос с идентификатором пользователя и выдаёт рекомендации,
#     учитывает историю пользователя,
#     смешивает онлайн- и офлайн-рекомендации.

import pandas as pd
import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager

import boto3
from dotenv import load_dotenv
import os
import io

from implicit.als import AlternatingLeastSquares

load_dotenv()

# Получаем уже созданный логгер "uvicorn.error", чтобы через него можно было логировать собственные сообщения в тот же поток,
# в который логирует и uvicorn
logger = logging.getLogger("uvicorn.error")


# Подключаемся к хранилищу данных для загрузки рекомендаций и модели ALS
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
S3_SERVICE_NAME = "s3"
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
session = boto3.session.Session()
s3 = session.client(
    service_name=S3_SERVICE_NAME,
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


# модель ALS для выдачи контентных онлайн-рекомендаций
als_model = AlternatingLeastSquares(
    factors=50, iterations=50, regularization=0.05, random_state=0
)
obj_als_model = s3.get_object(Bucket=BUCKET_NAME, Key="recsys/model/als_model_npz/")
als_model = als_model.load(io.BytesIO(obj_als_model["Body"].read()))
# als_model = als_model.load("als_model.npz")

# также понадобится items.parquet (для преобразования идентификаторов треков)
obj_items = s3.get_object(Bucket=BUCKET_NAME, Key="recsys/data/items_parquet/")
items = pd.read_parquet(io.BytesIO(obj_items["Body"].read()))
# items = pd.read_parquet("items.parquet")


# Подключение готовых рекомендаций (в отдельном классе)
# при запуске загружаются уже готовые рекомендации, а затем и отдаются при вызове /recommendations
class Recommendations:
    """
    Методы:

    load - загружает рекомендации указанного типа из файла.
    get - отдаёт персонализированные рекомендации, а если таковые не найдены, то рекомендации по умолчанию. Кроме того, он ведёт подсчёт количества отданных рекомендаций обоих типов.
    stats - выводит статистику по имеющимся счётчикам.
    """

    def __init__(self):

        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,  # счетчик персональных рекомендаций
            "request_default_count": 0,  # счетчик топ-рекомендаций
        }

    def load(self, type, path, **kwargs):
        """
        Загружает рекомендации из файла

        type == "personal" - персональные (при помощи ALS)
        type == "default" - топ-рекомендации
        """

        logger.info(f"Loading recommendations, type: {type}")

        if type == "personal":
            obj_personal_als_parquet = s3.get_object(
                Bucket=BUCKET_NAME, Key="recsys/recommendations/personal_als_parquet/"
            )
            self._recs[type] = pd.read_parquet(
                io.BytesIO(obj_personal_als_parquet["Body"].read()), **kwargs
            ).set_index("user_id")
            # self._recs[type] = self._recs[type].set_index("user_id")
        else:
            obj_top_popular_parquet = s3.get_object(
                Bucket=BUCKET_NAME, Key="recsys/recommendations/top_popular_parquet/"
            )
            self._recs[type] = pd.read_parquet(
                io.BytesIO(obj_top_popular_parquet["Body"].read()), **kwargs
            )
            # self._recs[type] = pd.read_parquet(path, **kwargs)

        logger.info(f"Loaded")

    def get(self, user_id: int, k: int = 100):
        """
        Возвращает список рекомендаций для пользователя
        """

        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
            logger.info(f"Found {len(recs)} personal recommendations!")
        except:
            recs = self._recs["default"]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
            logger.info(f"Found {len(recs)} TOP-recommendations!")

        if not recs:
            logger.error("No recommendations found")
            recs = []

        return recs

    def stats(self):

        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ")
        print(self._stats)
        return self._stats


# Класс Event Store - компонент, умеющий сохранять и выдавать последние события пользователя
class EventStore:
    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """

        user_events = self.events.get(user_id, [])
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        user_events = self.events.get(user_id, [])

        return user_events


# Функция ниже, которая передаётся как параметр FastAPI-объекту, выполняет свой код только при запуске приложения и при его остановке.
# При запуске приложения загружаем персональные и ТОП-рекомендации
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")

    # для оффайн-рекомендаций: автозагрузка перс-рекомендаций
    rec_store.load(
        type="personal",
        path="personal_als.parquet",
        columns=["user_id", "track_id", "score"],
    )
    # для оффайн-рекомендаций: автозагрузка топ-рекомендаций
    rec_store.load(
        type="default",
        path="top_popular.parquet",
        columns=["track_id", "popularity_weighted"],
    )

    yield
    logger.info("Stopping")


# загрузка готовых оффлайн-рекомендаций
rec_store = Recommendations()

# Event Store
events_store = EventStore()

# создаём приложение FastAPI
app = FastAPI(title="FastAPI-микросервис для выдачи рекомендаций", lifespan=lifespan)


@app.post("/recommendations", name="Получение рекомендаций для пользователя")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs_offline = rec_store.get(user_id, k)

    recs_online = await get_online_u2i(user_id, k, N=10)
    recs_online = recs_online["recs"]

    min_length = min(len(recs_offline), len(recs_online))

    logger.info(f"recs_offline {recs_offline}")
    logger.info(f"recs_online {recs_online}")

    recs_blended = []
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        recs_blended.append(recs_online[i])
        recs_blended.append(recs_offline[i])

    recs_blended = [recs_blended]

    logger.info(f"recs_blended {recs_blended}")

    # добавляем оставшиеся элементы в конец
    recs_blended.append(recs_offline[min_length:])
    recs_blended.append(recs_online[min_length:])

    # удаляем дубликаты
    recs_blended = dedup_ids(sum(recs_blended, []))

    # оставляем только первые k рекомендаций
    recs_blended[:k]

    # посмотрим, какие треки выдал итоговый-рекоммендатор
    if recs_blended:
        for i in recs_blended:
            print(
                "online rec track name: ",
                items.query("track_id == @i")["track_name"].to_list()[0],
            )
            print(
                "online rec artist name: ",
                items.query("track_id == @i")["artist_name"].to_list()[0],
            )

    return {"recs": recs_blended}


@app.post("/get_online_u2i")
async def get_online_u2i(user_id: int, k: int = 100, N: int = 10):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    # получаем список k-последних событий пользователя
    events = await get_user_events(user_id=user_id, k=k)
    events = events["events"]

    # получаем список из N треков, похожих на последние k, с которыми взаимодействовал пользователь
    sim_track_ids = []
    sim_track_scores = []
    if len(events) > 0:
        for track_id in events:
            sim_track_id, sim_track_score = await get_als_i2i(track_id, N=N)
            sim_track_ids.append(sim_track_id)
            sim_track_scores.append(sim_track_score)
        sim_track_ids = sum(sim_track_ids, [])
        sim_track_scores = sum(sim_track_scores, [])
    else:
        recs = []

    # сортируем похожие объекты по scores в убывающем порядке
    combined = list(zip(sim_track_ids, sim_track_scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)

    # посмотрим, какие треки выдал онлайн-рекоммендатор
    if recs:
        for i in recs:
            print(
                "online rec track name: ",
                items.query("track_id == @i")["track_name"].to_list()[0],
            )
            print(
                "online rec artist name: ",
                items.query("track_id == @i")["artist_name"].to_list()[0],
            )

    return {"recs": recs}


async def get_als_i2i(track_id: int, N: int = 1):
    """
    Выводит список идентификаторов похожих треков по track_id
    """

    track_id_enc = items.query("track_id == @track_id")["track_id_enc"].to_list()[0]
    similar_items = als_model.similar_items(track_id_enc, N=N)
    similar_tracks_enc = similar_items[0].tolist()[1 : N + 1]
    similar_tracks_scores = similar_items[1].tolist()[1 : N + 1]

    similar_tracks = []
    for i_enc in similar_tracks_enc:
        similar_tracks.append(
            items.query("track_id_enc == @i_enc")["track_id"].to_list()[0]
        )

    return similar_tracks, similar_tracks_scores


@app.post("/put_user_event")
async def put_user_event(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id
    """

    events_store.put(user_id, item_id)

    return {"result": "ok"}


@app.post("/get_user_events")
async def get_user_events(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    """

    events = events_store.get(user_id, k)

    return {"events": events}


@app.get("/load_recommendations", name="Загрузка рекомендаций из файла")
async def load_recommendations(rec_type: str, file_path: str):
    """
    Загружает оффлайн-рекомендации из файла (на случай, если файлы рекомендаций обновились)
    """

    if rec_type == "personal":
        columns = ["user_id", "track_id", "score"]
    else:
        columns = ["track_id", "popularity_weighted"]
    rec_store.load(
        type=rec_type,
        path=file_path,
        columns=columns,
    )


@app.get("/get_statistics", name="Получение статистики по рекомендациям")
async def get_statistics():
    """
    Выводит статистику по имеющимся счётчикам
    """

    return rec_store.stats()


def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids


# запуск сервиса
# uvicorn recommendations_service:app
# INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)

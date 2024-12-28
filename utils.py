import pandas as pd
import logging

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
obj_als_model = s3.get_object(Bucket=BUCKET_NAME, Key=os.environ.get("KEY_ALS_MODEL"))
als_model = als_model.load(io.BytesIO(obj_als_model["Body"].read()))

# также понадобится items.parquet (для преобразования идентификаторов треков)
obj_items_parquet = s3.get_object(
    Bucket=BUCKET_NAME, Key=os.environ.get("KEY_ITEMS_PARQUET")
)
items = pd.read_parquet(io.BytesIO(obj_items_parquet["Body"].read()))


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
                Bucket=BUCKET_NAME, Key=os.environ.get("KEY_PERSONAL_ALS_PARQUET")
            )
            self._recs[type] = pd.read_parquet(
                io.BytesIO(obj_personal_als_parquet["Body"].read()), **kwargs
            ).set_index("user_id")
        else:
            obj_top_popular_parquet = s3.get_object(
                Bucket=BUCKET_NAME, Key=os.environ.get("KEY_TOP_POPULAR_PARQUET")
            )
            self._recs[type] = pd.read_parquet(
                io.BytesIO(obj_top_popular_parquet["Body"].read()), **kwargs
            )

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


# загрузка готовых оффлайн-рекомендаций
rec_store = Recommendations()

# Event Store
events_store = EventStore()


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


def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids

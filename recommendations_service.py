# FastAPI-микросервис для выдачи рекомендаций, который:

#     принимает запрос с идентификатором пользователя и выдаёт рекомендации,
#     учитывает историю пользователя,
#     смешивает онлайн- и офлайн-рекомендации.

from utils import rec_store, events_store, items, dedup_ids, get_als_i2i
from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from dotenv import load_dotenv

load_dotenv()

# Получаем уже созданный логгер "uvicorn.error", чтобы через него можно было логировать собственные сообщения в тот же поток,
# в который логирует и uvicorn
logger = logging.getLogger("uvicorn.error")


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


# запуск сервиса
# uvicorn recommendations_service:app
# INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)

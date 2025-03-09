from typing import Union

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder

from news import store_news, fetch_and_store_news

from sample_read import read
from pymongo import MongoClient
from constants import MONGO_ATLAS_URI
from api_call import make_api_call

atlas_client = MongoClient(MONGO_ATLAS_URI)
local_client = MongoClient('localhost', 27017)

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/atlas_city_read/{city}")
def get_city_news(city: str):
    result = read(mongo_client = atlas_client, which_city=city)
    return result

@app.get("/local_city_read/{city}")
def get_city_news(city: str):
    result = read(mongo_client = local_client, which_city=city)
    return result

@app.get("/get_and_store_news/{city}")
def get_city_news(city: str):
    news = make_api_call(city)
    store_news(local_client, news, city, insert=False)
    return jsonable_encoder(news)

@app.get("/just_get_news/{city}")
def get_city_news(city: str):
    news = make_api_call(city)
    return news
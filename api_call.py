import requests
import json
# from constants import API_KEY
import os

API_KEY = os.environ.get('API_KEY')

BASE_URL = 'https://eventregistry.org/api/v1/article/getArticles'


def make_api_call(which_city: str):
    query = {
    "$query": {"locationUri": f"http://en.wikipedia.org/wiki/{which_city.replace(' ', '_')}"},
    "$filter": {"forceMaxDataTimeWindow": "31"}
    }

    params = {
        'query': json.dumps(query),
        'resultType': 'articles',
        'articlesSortBy': 'date',
        'apiKey': API_KEY
    }
    
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    news = make_api_call('Denver')
    print(news)
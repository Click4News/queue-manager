import requests
import json
# from constants import API_KEY
import os
from cities import CITIES_DICT
# API_KEY = os.environ.get('API_KEY')

BASE_URL = 'https://eventregistry.org/api/v1/article/getArticles'
API_KEY = 'f9a1b16e-6dd1-4f71-b009-f856160b2cf6'

def make_api_call(which_city: str, num_articles: int = 100):
    state = CITIES_DICT[which_city]
    which_city += f', {state}'
    print(which_city)
    query = {
        "$query": {"locationUri": f"http://en.wikipedia.org/wiki/{which_city.replace(' ', '_')}"},
        "$filter": {"forceMaxDataTimeWindow": "31"}
    }

    params = {
        'query': json.dumps(query),
        'resultType': 'articles',
        'articlesSortBy': 'date',
        'apiKey': API_KEY,
        'articlesCount': num_articles
    }

    print(params)
    
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    news = make_api_call('Huntsville', 1)
    print(json.dumps(news, indent=4))
    print(json.dumps(news['articles']['results']))
    print(len(news['articles']['results']))
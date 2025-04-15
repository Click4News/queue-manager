import requests
import json
# from constants import API_KEY
import os

API_KEY = os.environ.get('API_KEY')

BASE_URL = 'https://eventregistry.org/api/v1/article/getArticles'


def make_api_call(which_city: str, num_articles: int = 100):
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
    
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    news = make_api_call('Denver', 9)
    print(json.dumps(news, indent=4))
    print(len(news['articles']['results']))
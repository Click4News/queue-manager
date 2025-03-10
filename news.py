import requests
import json
from pymongo import MongoClient
import urllib.parse
from constants import atlas_pswd, MONGO_ATLAS_URI
from api_call import make_api_call
from bson import ObjectId

from geopy.geocoders import Nominatim 

geolocator = Nominatim(user_agent="Click4News")

def fetch_and_store_news(mongo_client, city_name):
    try:
        data = make_api_call(which_city=city_name)

        print("API Response:")
        print(json.dumps(data, indent=4))

        if 'articles' in data and 'results' in data['articles']:
            articles = data['articles']['results']
            if not articles:
                print("No articles found for this city.")
                return

            for article in articles:
                article['city'] = city_name

            # client = MongoClient(MONGO_URI)
            client = MongoClient('localhost', 27017)
            db = mongo_client['newsdata']
            collection = db['articles']
            collection.insert_many(articles)
            print(f"{len(articles)} articles inserted into MongoDB for city: {city_name}")

            return articles

        else:
            print("No articles found or incorrect API response.")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as e:
        print("Failed to fetch data:", e)
    except TypeError as te:
        print("Error with data format:", te)
    except Exception as ex:
        print(f"Failed for city : {city}\n\nError: {ex}")

def store_news(mongo_client, news, which_city: str, insert: bool = True):
    if 'articles' in news and 'results' in news['articles']:
        articles = news['articles']['results']
        if not articles:
            print("No articles found for this city.")
            return
        location = geolocator.geocode(f"{which_city}")
        lat, long = location.latitude, location.longitude

        geoJson = {
            "type": "Location",
            "geometry" : {
                "type": "Point",
                "coordinates": [lat, long]
            },
            "properties": {
                "name": f"{which_city}"
            }
        }

        for article in articles:
            article['city'] = which_city
            article['id'] = str(ObjectId())
            article['geoJson'] = geoJson

    if insert:
        db = mongo_client['newsdata']
        collection = db['articles']
        collection.insert_many(articles)
    
    return True


if __name__ == '__main__':
    cities = ['Denver']
    for city in cities: 
        store_news(city)
    
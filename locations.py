import json
import time
import os
from cities import CITIES_DICT
import requests
# from constants import mapbox_api_key

mapbox_api_key = os.environ.get('MAPBOX_API_KEY')

locations_file_name = "locations_new.json"


def pre_fetch_locationns():
    if os.path.exists(locations_file_name):
        with open(locations_file_name, "r") as f:
            data = json.load(f)
            f.close()
            return data
        
    city_coordinates = {}
    failed_cities = []
    for city, state in CITIES_DICT.items():
        try:
            # response = requests.get(f'https://api.mapbox.com/search/geocode/v6/forward?q={city}&proximity=ip&access_token={mapbox_api_key}')
            response = requests.get(f'https://api.mapbox.com/search/geocode/v6/forward?q={city}%2C%20{state}&proximity=ip&access_token={mapbox_api_key}')

            information = response.json()
            city_coordinates[city] = information['features'][0]['geometry']['coordinates']
            print(f'{city}:\t{information["features"][0]["geometry"]["coordinates"]}')
            time.sleep(1.1)
        except Exception as e:
            print(f"Error getting coordinates for {city}: {str(e)}")
            failed_cities.append(city)
            time.sleep(1.1)
    
    with open(locations_file_name, 'w') as f:
        json.dump(city_coordinates, f, indent=2)
    
    f.close()

    return city_coordinates

def test_api_available(city, state):
    response = requests.get(f'https://api.mapbox.com/search/geocode/v6/forward?q={city}%2C%20{state}&proximity=ip&access_token={mapbox_api_key}')
    print(response.status_code)


if __name__ == "__main__":
    test_api_available(city="Denver", state="Colorado")
    points = pre_fetch_locationns()
    print(points)
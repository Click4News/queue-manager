import json
import time
import os
from cities import CITIES
from geopy.geocoders import Nominatim
import requests
from constants import mapbox_api_key


def pre_fetch_locationns():
    if os.path.exists("locations.json"):
        with open("locations.json", "r") as f:
            data = json.load(f)
            f.close()
            return data
        
    city_coordinates = {}
    failed_cities = []
    for city in CITIES:    
        try:
            response = requests.get(f'https://api.mapbox.com/search/geocode/v6/forward?q={city}&proximity=ip&access_token={mapbox_api_key}')
            information = response.json()
            city_coordinates[city] = information['features'][0]['geometry']['coordinates']
            print(f'{city}:\t{information['features'][0]['geometry']['coordinates']}')
            time.sleep(1.1)
        except Exception as e:
            print(f"Error getting coordinates for {city}: {str(e)}")
            failed_cities.append(city)
            time.sleep(1.1)
    
    with open("locations.json", 'w') as f:
        json.dump(city_coordinates, f, indent=2)
    
    f.close()

    return city_coordinates

if __name__ == "__main__":
    points = pre_fetch_locationns()
    print(points)
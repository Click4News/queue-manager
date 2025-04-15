from typing import Union
from fastapi import FastAPI
from contextlib import asynccontextmanager
from api_call import make_api_call
from bson import ObjectId
from sqs_producer import *
from geopy.geocoders import Nominatim 
from apscheduler.schedulers.background import BackgroundScheduler
import time
from datetime import datetime
from cities import CITIES

from locations import pre_fetch_locationns

locations = pre_fetch_locationns()

location_names = locations.keys()

scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(scheduled_job, 'interval', minutes=30, id='news_fetch_job')
    scheduler.start()
    print("Scheduler started")

    scheduled_job()
    
    yield  # This is where the app runs
    

    scheduler.shutdown()
    print("Scheduler shut down")

app = FastAPI(lifespan=lifespan)

import threading

def process_city(city: str, num_articles: int = 100):
    """Process a single city's news and push to SQS queue"""
    try:
        print(f"Fetching news for {city}")
        news = make_api_call(city, num_articles)
        articles = news['articles']['results']
        
        # Get location data
        try:
            location = locations[city]
            lat, long = location[0], location[1]
            geoJson = {
                "type": "Location",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lat, long]
                },
                "properties": {
                    "name": f"{city}"
                }
            }
        except Exception as loc_error:
            print(f"Geolocation error for {city}: {str(loc_error)}")
            geoJson = {
                "type": "Location",
                "geometry": {
                    "type": "Point",
                    "coordinates": [0, 0]  # Default coordinates if geocoding fails
                },
                "properties": {
                    "name": f"{city}"
                }
            }
        
        # Process and push each article
        success_count = 0
        for article in articles:
            try:
                article['city'] = city
                article['id'] = str(ObjectId())
                article['geoJson'] = geoJson
                article['fetch_timestamp'] = datetime.now().isoformat()
                push_message_to_sqs('test-queue', article)
                print(f"Thread {threading.get_ident()}: processed for {article['city']} with id: {article['id']}")
                success_count += 1
            except Exception as article_error:
                print(f"Error processing article for {city}: {str(article_error)}")
        
        print(f"Successfully pushed {success_count}/{len(articles)} articles for {city}")
        return success_count
    except Exception as e:
        print(f"Failed to process {city}: {str(e)}")
        return 0

from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

def scheduled_job():
    """Job to run every 30 minutes, parallelized with ThreadPoolExecutor"""
    print(f"Starting scheduled job at {datetime.now()}")
    total_articles = 0
    failed_cities = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_city = {executor.submit(process_city, city): city for city in CITIES}
        
        for future in concurrent.futures.as_completed(future_to_city):
            city = future_to_city[future]
            try:
                articles_processed = future.result()
                total_articles += articles_processed
            except Exception as e:
                print(f"Error processing city {city}: {str(e)}")
                failed_cities.append(city)
    
    print(f"Scheduled job completed. Processed {total_articles} articles.")
    if failed_cities:
        print(f"Failed to process these cities: {', '.join(failed_cities)}")

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.get("/test_sqs/")
def test_queue_push():
    try: 
        push_message_to_sqs('test-queue', 'Test message')
        return {"Message": "Worked"}
    except Exception as e:
        error_message = f"Failed to push message to SQS: {str(e)}"
        return {"Error": str(e), "Details": error_message}
    

@app.get("/get_city_locations/{city}/{num}")
def get_city_loc(city: str, num: int = 5):
    return {"city": city, "number of articles": num, "location": locations[city]}

@app.get("/just_get_news/{city}/{num}")
def get_city_news(city: str, num: int = 100):
    try: 
        news = make_api_call(city, num)
    except Exception as e:
        print(f"News API call error for {city}: {str(e)}")
        return {"Error": str(e)}
        
    articles = news['articles']['results']
    try:
        # location = geolocator.geocode(f"{city}")
        location = locations[city]
        lat, long = location[0], location[1]
        geoJson = {
            "type": "Location",
            "geometry": {
                "type": "Point",
                "coordinates": [lat, long]
            },
            "properties": {
                "name": f"{city}"
            }
        }
        
        for article in articles:
            article['city'] = city
            article['id'] = str(ObjectId())
            article['geoJson'] = geoJson
            push_message_to_sqs('test-queue', article)
        
        print(f'Processed {len(articles)} articles for {city} via API endpoint')
        return news
    except Exception as e:
        print(f"Error in processing articles for {city}: {str(e)}")
        return {"Error": str(e), "articles": news.get('articles', {}).get('results', [])}

@app.post("/trigger_job/")
def trigger_job():
    try:
        scheduled_job()
        return {"status": "success", "message": "Job triggered successfully"}
    except Exception as e:
        print(f"Error triggering job manually: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/scheduler_status/")
def scheduler_status():
    job = scheduler.get_job('news_fetch_job')
    if job:
        return {
            "status": "running" if scheduler.running else "stopped",
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None
        }
    return {"status": "job not found"}
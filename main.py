from typing import Union, List, Dict, Any
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from api_call import make_api_call
from bson import ObjectId
from sqs_producer import push_message_to_sqs
from apscheduler.schedulers.background import BackgroundScheduler
import time
from datetime import datetime
from cities import CITIES
from locations import pre_fetch_locationns
import threading
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Pre-fetch locations
locations = pre_fetch_locationns()
location_names = locations.keys()

# Initialize scheduler but don't schedule any jobs yet
scheduler = BackgroundScheduler()

# Initialize results dictionary for tracking article counts by city
# This will be populated when the job runs
results = {city: 0 for city in CITIES}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager to handle startup and shutdown events."""
    # Start the scheduler but don't schedule any jobs yet
    # Jobs will only be added when the /start-scheduler-job endpoint is called
    scheduler.start()
    logger.info("Scheduler started (no jobs scheduled)")
    
    yield  # This is where the app runs
    
    scheduler.shutdown()
    logger.info("Scheduler shut down")

app = FastAPI(lifespan=lifespan)

def get_location_data(city: str) -> Dict:
    """Get location data for a city and format as GeoJSON."""
    try:
        location = locations[city]
        lat, long = location[0], location[1]
        return {
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
        logger.error(f"Geolocation error for {city}: {str(loc_error)}")
        return {
            "type": "Location",
            "geometry": {
                "type": "Point",
                "coordinates": [0, 0]  # Default coordinates if geocoding fails
            },
            "properties": {
                "name": f"{city}"
            }
        }

def prepare_article(article: Dict, city: str, geo_json: Dict) -> Dict:
    """Prepare an article with additional metadata."""
    article['city'] = city
    article['id'] = str(ObjectId())
    article['geoJson'] = geo_json
    article['fetch_timestamp'] = datetime.now().isoformat()
    article['type'] = "CREATE"
    return article

def push_article_to_queue(article: Dict) -> bool:
    """Push a single article to SQS queue."""
    try:
        push_message_to_sqs('test-queue', article)
        logger.debug(f"Thread {threading.get_ident()}: pushed article {article['id']}")
        return True
    except Exception as e:
        logger.error(f"Error pushing article to queue: {str(e)}")
        return False

def fetch_city_news(city: str, num_articles: int = 100) -> List[Dict]:
    """Fetch news for a city and prepare articles with metadata."""
    try:
        logger.info(f"Fetching news for {city}")
        news = make_api_call(city, num_articles)
        articles = news['articles']['results']
        
        # Get location data
        geo_json = get_location_data(city)
        
        # Prepare articles with metadata
        prepared_articles = [
            prepare_article(article, city, geo_json) 
            for article in articles
        ]
        
        logger.info(f"Fetched and prepared {len(prepared_articles)} articles for {city}")
        return prepared_articles
    except Exception as e:
        logger.error(f"Failed to fetch news for {city}: {str(e)}")
        return []

def scheduled_job():
    """Job to run on schedule - fetches news for all cities then pushes articles in parallel."""
    start_time = datetime.now()
    logger.info(f"Starting scheduled job at {start_time}")
    
    # Step 1: Fetch all articles from all cities
    all_articles = []
    failed_cities = []
    
    for city in CITIES:
        try:
            city_articles = fetch_city_news(city)
            all_articles.extend(city_articles)
            results[city] = len(city_articles)
        except Exception as e:
            logger.error(f"Error fetching news for {city}: {str(e)}")
            failed_cities.append(city)
            results[city] = 0
    
    # Step 2: Push articles to SQS in parallel using ThreadPoolExecutor
    success_count = 0
    logger.info(f"Pushing {len(all_articles)} articles to SQS queue using multiple threads")
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_article = {
            executor.submit(push_article_to_queue, article): article 
            for article in all_articles
        }
        
        for future in concurrent.futures.as_completed(future_to_article):
            article = future_to_article[future]
            try:
                if future.result():
                    success_count += 1
            except Exception as e:
                logger.error(f"Error pushing article to queue: {str(e)}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Scheduled job completed in {duration:.2f} seconds.")
    logger.info(f"Successfully pushed {success_count}/{len(all_articles)} articles to SQS.")
    
    if failed_cities:
        logger.info(f"Failed to process these cities: {', '.join(failed_cities)}")

@app.get("/")
def read_root():
    return {
        "What is the Scheduled Job?": "Every 30 minutes, news for all cities are queried from the NewsAPI, then all articles are pushed to the SQS queue using multiple threads.",
        "/health": "Health check endpoint",
        "/test_sqs": "Tests if SQS queue is up and messages can be pushed",
        "/scheduler_status": "Status of the scheduler job (if any)",
        "/start-scheduler-job": "Starts the scheduled job",
        "/stop-scheduler-job": "Stops the scheduled job"
    }

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
    
@app.get("/user_news")
def push_user_news(news: dict):
    push_message_to_sqs('test-queue', news)
    return {"status": "success"}

@app.get("/get_city_locations/{city}/{num}")
def get_city_loc(city: str, num: int = 5):
    return {"city": city, "number of articles": num, "location": locations[city]}

@app.get("/just_get_news/{city}/{num}")
def get_city_news(city: str, num: int = 100):
    try:
        articles = fetch_city_news(city, num)
        
        # Push articles to SQS in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(push_article_to_queue, articles))
        
        logger.info(f'Processed {len(articles)} articles for {city} via API endpoint')
        return {"status": "success", "article_count": len(articles)}
    except Exception as e:
        logger.error(f"Error in processing articles for {city}: {str(e)}")
        return {"Error": str(e)}

@app.post("/trigger_job/")
def trigger_job():
    try:
        scheduled_job()
        return {"status": "success", "message": "Job triggered successfully"}
    except Exception as e:
        logger.error(f"Error triggering job manually: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/scheduler_status/")
def scheduler_status():
    job = scheduler.get_job('news_fetch_job')
    if job:
        return {
            "status": "running" if scheduler.running else "stopped",
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "results": results
        }
    return {"status": "job not found"}

@app.get("/start-scheduler-job")
async def start_scheduled_job():
    """Start running the job on a schedule"""
    try:
        # Check if the job already exists
        job = scheduler.get_job('news_fetch_job')
        if job:
            return {"status": "warning", "message": "Job is already scheduled"}
        
        # Add the job to the scheduler
        scheduler.add_job(scheduled_job, 'interval', minutes=30, id='news_fetch_job')
        logger.info("Scheduled job started - will run every 30 minutes")
        
        # Optionally, run the job immediately
        scheduled_job()
        # If you want to run immediately, uncomment the line above
        
        return {"status": "success", "message": "Job scheduled successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to schedule job: {str(e)}")

@app.get("/stop-scheduler-job")
async def stop_scheduled_job():
    """Stop the scheduled job"""
    try:
        # Remove the job from the scheduler
        scheduler.remove_job('news_fetch_job')
        return {"status": "success", "message": "Job stopped successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop job: {str(e)}")
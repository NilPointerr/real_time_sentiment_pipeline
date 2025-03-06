from fastapi import APIRouter, BackgroundTasks
from data_streaming.reddit_cleaner import clean_reddit_data
from config.settings import settings
router = APIRouter()

KAFKA_BROKER = settings.KAFKA_BROKER
INPUT_TOPIC = settings.INPUT_TOPIC
OUTPUT_TOPIC = settings.OUTPUT_TOPIC

@router.post("/start_cleaning")
async def start_cleaning(background_tasks: BackgroundTasks):
    """
    Endpoint to start Reddit data cleaning using PySpark.
    """
    background_tasks.add_task(clean_reddit_data, KAFKA_BROKER, INPUT_TOPIC, OUTPUT_TOPIC)
    
    return {"message": "Reddit data cleaning started in the background."}

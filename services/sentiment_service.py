from fastapi import APIRouter, BackgroundTasks
from sentiment_analysis.bert_inference import train_sentiment_model
from config.settings import settings

router = APIRouter()

KAFKA_BROKER = settings.KAFKA_BROKER
OUTPUT_TOPIC = settings.OUTPUT_TOPIC
SENTIMENT_TOPIC = settings.SENTIMENT_TOPIC

@router.post("/start_training")
async def start_training(background_tasks: BackgroundTasks):
    """
    Endpoint to start training the sentiment analysis model using Spark.
    """
    background_tasks.add_task(train_sentiment_model, KAFKA_BROKER, OUTPUT_TOPIC, SENTIMENT_TOPIC)
    return {"message": "Model training has started in the background."}

@router.post("/stop_training")
async def stop_training():
    """
    Endpoint to stop the running training job.
    """
    from sentiment_analysis.bert_inference import query
    if query is not None:
        query.stop()
        return {"message": "Model training has been stopped successfully."}
    else:
        return {"message": "No training job is currently running."}

@router.get("/status_training")
async def status_training():
    """
    Check the status of the training job.
    """
    from sentiment_analysis.bert_inference import query
    if query is not None and query.isActive:
        return {"status": "running"}
    else:
        return {"status": "stopped"}

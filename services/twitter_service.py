from fastapi import APIRouter, BackgroundTasks
from data_streaming.twitter_producer import stream_twitter_data

router = APIRouter()

# Dictionary to keep track of running streams
active_streams = {}

@router.post("/start_twitter_stream")
async def start_twitter_stream(keyword: str, background_tasks: BackgroundTasks):
    """
    Start streaming tweets for a given keyword and send them to Kafka.
    """
    if keyword in active_streams:
        return {"message": f"Streaming for keyword '{keyword}' is already running."}

    # Add Twitter stream as a background task
    background_tasks.add_task(stream_twitter_data, keyword)
    active_streams[keyword] = True

    return {"message": f"Started streaming tweets for keyword: {keyword}"}

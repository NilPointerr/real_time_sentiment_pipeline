from fastapi import APIRouter, BackgroundTasks
from data_streaming.reddit_producer import stream_reddit_posts, streaming_active

router = APIRouter()

@router.post("/start_reddit_stream")
async def start_reddit_stream(background_tasks: BackgroundTasks):
    """
    Start streaming posts from all subreddits (r/all) to Kafka.
    Runs as a background task.
    """
    if streaming_active:
        return {"message": "Streaming is already running."}

    background_tasks.add_task(stream_reddit_posts)
    return {"message": "Started streaming Reddit posts from r/all."}

@router.post("/stop_reddit_stream")
async def stop_reddit_stream():
    """
    Stop streaming posts from Reddit.
    """
    global streaming_active
    if not streaming_active:
        return {"message": "No active Reddit stream found."}

    streaming_active = False  # Set flag to stop streaming
    return {"message": "Stopped Reddit streaming."}

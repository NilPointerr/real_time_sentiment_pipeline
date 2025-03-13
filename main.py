from fastapi import FastAPI
from services.kafka_service import router as kafka_router
from services.twitter_service import router as twitter_router
from services.cleaning_service import router as cleaning_router
from services.sentiment_service import router as sentiment_router
# import uvicorn

app = FastAPI(title="Real-Time Sentiment Analysis API")

@app.get("/")
def root():
    return {"message": "Real-Time Sentiment Analysis API is running"}

app.include_router(kafka_router, prefix="/kafka", tags=["Kafka"])
app.include_router(twitter_router, prefix="/twitter", tags=["Twitter"])
app.include_router(cleaning_router, prefix="/cleaning", tags=["Cleaning"])
app.include_router(sentiment_router,prefix="/sentiment",tags=["TrainData"])

# Start Kafka Consumer in the background
# @app.on_event("startup")
# def startup_event():
#     start_kafka_consumer()

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
 
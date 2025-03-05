from fastapi import FastAPI
from consumers.kafka_consumer import start_kafka_consumer
from services.kafka_service import router as kafka_router

# import uvicorn

app = FastAPI(title="Real-Time Sentiment Analysis API")

@app.get("/")
def root():
    return {"message": "Real-Time Sentiment Analysis API is running"}

app.include_router(kafka_router, prefix="/kafka", tags=["Kafka"])



# Start Kafka Consumer in the background
# @app.on_event("startup")
# def startup_event():
#     start_kafka_consumer()

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
 
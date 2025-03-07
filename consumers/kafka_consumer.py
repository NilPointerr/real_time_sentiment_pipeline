# from kafka import KafkaConsumer
# import json
# from services.cleaning_service import clean_text

# # Kafka Consumer
# consumer = KafkaConsumer(
#     "twitter_stream",
#     "reddit_stream",
#     bootstrap_servers="kafka:9092",
#     auto_offset_reset="earliest",
#     enable_auto_commit=True,
#     value_deserializer=lambda x: json.loads(x.decode("utf-8"))
# )

# def process_message(message):
#     cleaned_text = clean_text(message["text"])
#     print(f"Cleaned Data: {cleaned_text}")  # Later, save to DB

# # Listen for messages
# for message in consumer:
#     process_message(message.value)


def start_kafka_consumer():
    pass
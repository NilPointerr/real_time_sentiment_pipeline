# import json
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf
# from pyspark.sql.types import StringType
# from config import KAFKA_BROKER, OUTPUT_TOPIC, SENTIMET_TOPIC
# from transformers import pipeline

# # Load the pre-trained BERT model
# sentiment_pipeline = pipeline("sentiment-analysis", model="bert-base-uncased")

# # Initialize Spark
# spark = SparkSession.builder \
#     .appName("SentimentAnalysis") \
#     .master("local[*]") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")

# # UDF to apply sentiment analysis

# def predict_sentiment(text):
#     result = sentiment_pipeline(text)
#     sentiment = result[0]['label'].lower()  # Convert label to lowercase
#     return sentiment

# sentiment_udf = udf(predict_sentiment, StringType())

# # Read from Kafka
# df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", OUTPUT_TOPIC) \
#     .option("startingOffsets", "latest") \
#     .load()

# df = df.selectExpr("CAST(value AS STRING) as json")

# def extract_text(json_str):
#     try:
#         data = json.loads(json_str)
#         return data.get("text", "")
#     except Exception as e:
#         return ""

# extract_text_udf = udf(extract_text, StringType())

# df = df.withColumn("text", extract_text_udf(col("json")))
# df = df.withColumn("sentiment", sentiment_udf(col("text")))

# # Write sentiment results to Kafka
# query = df.selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("topic", SENTIMET_TOPIC) \
#     .option("checkpointLocation", "/tmp/kafka_sentiment_checkpoint/") \
#     .start()

# query.awaitTermination()


import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from transformers import pipeline

# Load the pre-trained BERT model
sentiment_pipeline = pipeline("sentiment-analysis", model="bert-base-uncased")

# Initialize Spark
spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Global reference to the query for controlling the streaming job
query = None

# UDF to apply sentiment analysis
def predict_sentiment(text):
    result = sentiment_pipeline(text)
    sentiment = result[0]['label'].lower()  # Convert label to lowercase
    return sentiment

sentiment_udf = udf(predict_sentiment, StringType())

def extract_text(json_str):
    try:
        data = json.loads(json_str)
        return data.get("text", "")
    except Exception as e:
        return ""

extract_text_udf = udf(extract_text, StringType())

def train_sentiment_model(kafka_broker, input_topic, output_topic):
    global query

    # Read from Kafka
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Extract text from Kafka messages
    df = df.selectExpr("CAST(value AS STRING) as json")
    df = df.withColumn("text", extract_text_udf(col("json")))
    df = df.withColumn("sentiment", sentiment_udf(col("text")))

    # Write sentiment results to Kafka
    query = df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", output_topic) \
        .option("checkpointLocation", "/tmp/kafka_sentiment_checkpoint/") \
        .start()

    # Keep the streaming job running
    query.awaitTermination()


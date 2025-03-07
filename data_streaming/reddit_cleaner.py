# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, from_unixtime, expr
# import logging
# from pyspark.sql.types import StructType, StringType, IntegerType, LongType

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def clean_reddit_data(kafka_broker: str, input_topic: str, output_topic: str):
#     """
#     Cleans Reddit data streamed from Kafka and writes back the cleaned data.
#     """
#     try:
#         # Initialize Spark Session
#         spark = SparkSession.builder \
#             .appName("RedditDataCleaning") \
#             .master("local[*]") \
#             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#             .getOrCreate()

#         spark.sparkContext.setLogLevel("ERROR")  
#         logger.info("✅ Spark session initialized successfully.")

#         # Define Schema (Only required columns)
#         reddit_schema = StructType() \
#             .add("title", StringType(), True) \
#             .add("text", StringType(), True) \
#             .add("author", StringType(), True) \
#             .add("subreddit", StringType(), True) \
#             .add("comments", IntegerType(), True)

#         # Read streaming data from Kafka
#         reddit_df = spark.readStream.format("kafka") \
#             .option("kafka.bootstrap.servers", kafka_broker) \
#             .option("subscribe", input_topic) \
#             .option("startingOffsets", "earliest") \
#             .load()

#         logger.info("✅ Streaming data successfully read from Kafka topic: %s", input_topic)

#         # Convert Kafka value (binary) to JSON
#         reddit_df = reddit_df.selectExpr("CAST(value AS STRING) as json")

#         # Convert JSON to structured DataFrame
#         reddit_df = reddit_df.select(from_json(col("json"), reddit_schema).alias("data")).select("data.*")

#         logger.info("✅ JSON data successfully parsed into DataFrame.")

#         # 🧹 Step 1: Handle missing values
#         reddit_df = reddit_df.fillna({
#             "title": "No Title", 
#             "text": "No text", 
#             "author": "unknown", 
#             "subreddit": "unknown", 
#             "comments": 0
#         })
#         # 🧹 Step 2: Remove duplicates
#         reddit_df = reddit_df.dropDuplicates(["title", "text"])

#         logger.info("✅ Data cleaning steps completed successfully.")
        
#         # 📝 Save cleaned data to Kafka as a stream
#         query = reddit_df.selectExpr("to_json(struct(*)) AS value") \
#             .writeStream \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", kafka_broker) \
#             .option("topic", output_topic) \
#             .option("checkpointLocation", "/tmp/kafka_checkpoint/") \
#             .outputMode("append").start()

#         logger.info("✅ Cleaned data successfully sent to Kafka topic: %s", output_topic)

#         query.awaitTermination()

#     except Exception as e:
#         logger.error("❌ Error during data cleaning: %s", str(e), exc_info=True)

#     finally:
#         spark.stop()
#         logger.info("✅ Spark session stopped.")




from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, udf
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType
from langdetect import detect
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define UDF to detect English text
def detect_english(text):
    try:
        return detect(text) == "en"
    except:
        return False  # If detection fails, assume non-English

is_english_udf = udf(detect_english, BooleanType())  # Ensure BooleanType

def clean_reddit_data(kafka_broker: str, input_topic: str, output_topic: str):
    try:
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("RedditDataCleaning") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logger.info("✅ Spark session initialized successfully.")

        # Define Schema
        reddit_schema = StructType() \
            .add("title", StringType(), True) \
            .add("text", StringType(), True) \
            .add("author", StringType(), True) \
            .add("subreddit", StringType(), True) \
            .add("comments", IntegerType(), True)

        # Read streaming data from Kafka
        reddit_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", input_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        logger.info("✅ Streaming data successfully read from Kafka topic: %s", input_topic)

        # Convert Kafka value (binary) to JSON
        reddit_df = reddit_df.selectExpr("CAST(value AS STRING) as json")
        reddit_df = reddit_df.select(from_json(col("json"), reddit_schema).alias("data")).select("data.*")

        logger.info("✅ JSON data successfully parsed into DataFrame.")

        # 🧹 Step 1: Handle missing values
        reddit_df = reddit_df.fillna({
            "title": "No Title",
            "text": "No text",
            "author": "unknown",
            "subreddit": "unknown",
            "comments": 0
        })

        # 🧹 Step 2: Remove newlines from text and title
        reddit_df = reddit_df.withColumn("title", regexp_replace(col("title"), "\n", " ")) \
                             .withColumn("text", regexp_replace(col("text"), "\n", " "))

        # 🧹 Step 3: Remove duplicates
        reddit_df = reddit_df.dropDuplicates(["title", "text"])

        # 🧹 Step 4: Filter only English data
        reddit_df = reddit_df.filter(is_english_udf(col("text")))

        logger.info("✅ Data cleaning steps completed successfully.")

        # 📝 Save cleaned data to Kafka as a stream
        query = reddit_df.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("topic", output_topic) \
            .option("checkpointLocation", "/tmp/kafka_checkpoint/") \
            .outputMode("append").start()

        logger.info("✅ Cleaned data successfully sent to Kafka topic: %s", output_topic)

        query.awaitTermination()

    except Exception as e:
        logger.error("❌ Error during data cleaning: %s", str(e), exc_info=True)

    finally:
        spark.stop()
        logger.info("✅ Spark session stopped.")

# import praw
# from kafka import KafkaProducer
# import json
# import time
# from config.settings import settings

# print("settings.KAFKA_BROKER",settings.KAFKA_BROKER)
# # Initialize Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers=settings.KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# # Initialize Reddit API Client
# reddit = praw.Reddit(
#     client_id=settings.REDDIT_CLIENT_ID,
#     client_secret=settings.REDDIT_CLIENT_SECRET,
#     user_agent=settings.REDDIT_USER_AGENT,
# )

# def stream_reddit_posts(subreddit_name: str):
#     subreddit = reddit.subreddit(subreddit_name)


#     # subreddit = reddit.subreddit("technology")
#     for post in subreddit.new(limit=5):
#         print(post.title)
#     print("starting stream------------")

#     # for post in subreddit.stream.submissions(skip_existing=True):
#     #     print("New post received:", post.title)

#     """
#     Streams new Reddit posts from a specified subreddit and sends them to Kafka.
#     """
#     print("subreddit--------",subreddit)
#     print("Waiting for new posts...")
#     for post in subreddit.stream.submissions():
#         print("New post received:", post.title)
#         data = {
#             "id": post.id,
#             "title": post.title,
#             "text": post.selftext,
#             "author": post.author.name if post.author else "unknown",
#             "subreddit": subreddit_name,
#             "timestamp": post.created_utc,
#             "url": post.url,
#             "score": post.score,
#             "comments": post.num_comments,
#             "source": "reddit"
#         }
#         print("data------",data)
#         # Send data to Kafka topic 'redditData'
#         # producer.send("redditData", value=data)
#         # print(f"Sent post to Kafka: {post.title}")
#         # # Prevent hitting API rate limits
#         # time.sleep(1)

#         # Send data to Kafka topic 'redditData'
#         future = producer.send("redditData", value=data)
        
#         # Handle errors
#         try:
#             record_metadata = future.get(timeout=10)  # Wait for send confirmation
#             print(f"✅ Sent post to Kafka: {post.title} | Partition: {record_metadata.partition}")
#         except Exception as e:
#             print(f"❌ Error sending post: {e}")






# import praw
# from kafka import KafkaProducer
# import json
# import time
# from config.settings import settings

# print("settings.KAFKA_BROKER", settings.KAFKA_BROKER)

# # Initialize Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers=settings.KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# # Initialize Reddit API Client
# reddit = praw.Reddit(
#     client_id=settings.REDDIT_CLIENT_ID,
#     client_secret=settings.REDDIT_CLIENT_SECRET,
#     user_agent=settings.REDDIT_USER_AGENT,
# )

# # Global flag to control streaming
# streaming_active = False

# def stream_reddit_posts():
#     """
#     Streams new Reddit posts from all subreddits (r/all) and sends them to Kafka.
#     """
#     global streaming_active
#     if streaming_active:
#         print("⚠️ Streaming is already running.")
#         return

#     print("🚀 Starting Reddit stream from r/all...")
#     streaming_active = True

#     subreddit = reddit.subreddit("all")  # Get posts from all subreddits

#     try:
#         for post in subreddit.stream.submissions(skip_existing=True):
#             if not streaming_active:
#                 print("⏹️ Stopping Reddit stream...")
#                 break

#             data = {
#                 "id": post.id,
#                 "title": post.title,
#                 "text": post.selftext,
#                 "author": post.author.name if post.author else "unknown",
#                 "subreddit": post.subreddit.display_name,
#                 "timestamp": post.created_utc,
#                 "url": post.url,
#                 "score": post.score,
#                 "comments": post.num_comments,
#                 "source": "reddit"
#             }

#             future = producer.send("redditData", value=data)

#             # Handle message confirmation
#             try:
#                 record_metadata = future.get(timeout=10)
#                 print(f"✅ Sent to Kafka: {post.title} | Subreddit: {post.subreddit.display_name} | Partition: {record_metadata.partition}")
#             except Exception as e:
#                 print(f"❌ Error sending post: {e}")

#             time.sleep(1)  # Prevent API rate limiting

#     except Exception as e:
#         print(f"🔥 Error in streaming: {e}")
#     finally:
#         streaming_active = False
#         print("🔴 Reddit streaming stopped.")




import praw
from kafka import KafkaProducer
import json
import time
import threading
from config.settings import settings
from queue import Queue

print("settings.KAFKA_BROKER", settings.KAFKA_BROKER)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,  # Small delay to allow batching
    batch_size=16384  # Increase batch size for better throughput
)

# Initialize Reddit API Client
reddit = praw.Reddit(
    client_id=settings.REDDIT_CLIENT_ID,
    client_secret=settings.REDDIT_CLIENT_SECRET,
    user_agent=settings.REDDIT_USER_AGENT,
)

# Global flag to control streaming
streaming_active = False
data_queue = Queue()  # Queue for storing posts before sending to Kafka

def kafka_worker():
    """ Background thread to send data from the queue to Kafka asynchronously. """
    while streaming_active or not data_queue.empty():
        try:
            data = data_queue.get(timeout=2)  # Wait max 2 sec for new data
            future = producer.send("redditData", value=data)
            future.get(timeout=10)  # Forces error to be raised if Kafka fails
            data_queue.task_done()
        except Exception as e:
            # print(f"❌ Kafka Send Error: {e}")
            pass

def stream_reddit_posts():
    """
    Streams new Reddit posts from all subreddits (r/all) and sends them to Kafka.
    """
    global streaming_active
    if streaming_active:
        print("⚠️ Streaming is already running.")
        return

    print("🚀 Starting Reddit stream from r/all...")
    streaming_active = True

    # Start a background thread to process Kafka messages
    kafka_thread = threading.Thread(target=kafka_worker, daemon=True)
    kafka_thread.start()

    subreddit = reddit.subreddit("all")  # Get posts from all subreddits

    try:
        for post in subreddit.stream.submissions(skip_existing=True, pause_after=2):
            if not streaming_active:
                print("⏹️ Stopping Reddit stream...")
                break

            data = {
                "id": post.id,
                "title": post.title,
                "text": post.selftext,
                "author": post.author.name if post.author else "unknown",
                "subreddit": post.subreddit.display_name,
                "timestamp": post.created_utc,
                "url": post.url,
                "score": post.score,
                "comments": post.num_comments,
                "source": "reddit"
            }

            # Put data into the queue instead of waiting for Kafka
            data_queue.put(data)

            # print(f"✅ Queued: {post.title} | Subreddit: {post.subreddit.display_name}")

    except Exception as e:
        print(f"🔥 Error in streaming: {e}")
    finally:
        streaming_active = False
        data_queue.join()  # Wait until queue is fully processed
        print("🔴 Reddit streaming stopped.")


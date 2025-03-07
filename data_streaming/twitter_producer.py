import ssl
import json
import tweepy
from kafka import KafkaProducer
from config.settings import settings



# Twitter API credentials
API_KEY = settings.TWITTER_API_KEY
API_SECRET = settings.TWITTER_API_SECRET
ACCESS_TOKEN = settings.TWITTER_ACCESS_TOKEN
ACCESS_SECRET = settings.TWITTER_ACCESS_SECRET
BEARER_TOKEN = settings.TWITTER_BEARER_TOKEN

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

ssl._create_default_https_context = ssl._create_unverified_context

client = tweepy.Client(bearer_token=BEARER_TOKEN)

# Custom Twitter Stream Listener
class TwitterStream(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print(f"New Tweet: {tweet.text}")
        data = {
            "id": tweet.id,
            "text": tweet.text,
            "author_id": tweet.author_id,
            "timestamp": tweet.created_at.isoformat(),
            "source": "twitter"
        }

        # Send data to Kafka
        future = producer.send("twitterData", value=data)
        try:
            record_metadata = future.get(timeout=10)
            print(f"✅ Sent tweet to Kafka | Partition: {record_metadata.partition}")
        except Exception as e:
            print(f"❌ Error sending tweet: {e}")

# Start streaming function
def stream_twitter_data(keyword: str):
    stream = TwitterStream(BEARER_TOKEN)
    stream.add_rules(tweepy.StreamRule(keyword))
    stream.filter()

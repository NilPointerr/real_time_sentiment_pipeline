# from pydantic import BaseSettings
from pydantic_settings import BaseSettings  

class Settings(BaseSettings):
    KAFKA_BROKER: str
    POSTGRES_URL: str
    REDIS_URL: str
    INPUT_TOPIC : str
    OUTPUT_TOPIC : str

    TWITTER_API_KEY: str
    TWITTER_API_SECRET: str
    TWITTER_ACCESS_TOKEN: str
    TWITTER_ACCESS_SECRET: str
    TWITTER_BEARER_TOKEN: str

    REDDIT_CLIENT_ID: str
    REDDIT_CLIENT_SECRET: str
    REDDIT_USER_AGENT: str

    class Config:
        env_file = ".env"

settings = Settings()

# import os
# from dotenv import load_dotenv
# # from pydantic import BaseSettings
# from pydantic_settings import BaseSettings  

# # Load environment variables from .env file
# load_dotenv()

# class Settings(BaseSettings):
#     KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")
#     POSTGRES_URL: str = os.getenv("POSTGRES_URL", "postgresql://root:root@postgres:5432/sentiment_db")
#     REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")

#     TWITTER_API_KEY: str = os.getenv("TWITTER_API_KEY", "")
#     TWITTER_API_SECRET: str = os.getenv("TWITTER_API_SECRET", "")
#     TWITTER_ACCESS_TOKEN: str = os.getenv("TWITTER_ACCESS_TOKEN", "")
#     TWITTER_ACCESS_SECRET: str = os.getenv("TWITTER_ACCESS_SECRET", "")

#     REDDIT_CLIENT_ID: str = os.getenv("REDDIT_CLIENT_ID", "")
#     REDDIT_CLIENT_SECRET: str = os.getenv("REDDIT_CLIENT_SECRET", "")
#     REDDIT_USER_AGENT: str = os.getenv("REDDIT_USER_AGENT", "")

# settings = Settings()

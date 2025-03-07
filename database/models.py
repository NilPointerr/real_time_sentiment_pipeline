from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class SentimentData(Base):
    __tablename__ = "sentiment_data"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, index=True)  # Twitter or Reddit
    text = Column(String)
    sentiment_score = Column(Float)
    created_at = Column(DateTime, default=datetime.now())

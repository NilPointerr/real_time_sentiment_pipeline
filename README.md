real-time-sentiment-analysis/
│── config/                     # Configuration files (Kafka, DB, Redis)
│   ├── settings.py             # App settings & environment variables
│   ├── kafka_config.py         # Kafka-related configurations
│   ├── db_config.py            # PostgreSQL & Redis setup
│── data_streaming/             # Kafka Producers for Twitter & Reddit
│   ├── twitter_producer.py     # Streams live tweets to Kafka
│   ├── reddit_producer.py      # Streams Reddit posts to Kafka
│── services/                   # Core API Services
│   ├── cleaning_service.py     # FastAPI data cleaning service
│   ├── sentiment_service.py    # NLP sentiment analysis
│── database/                   # Database Models & Interactions
│   ├── models.py               # SQLAlchemy models
│   ├── schemas.py              # Pydantic schemas
│   ├── crud.py                 # Database operations
│── consumers/                  # Kafka Consumers
│   ├── kafka_consumer.py       # Consumes messages from Kafka
│── dashboard/                  # Frontend dashboard (React/Vue)
│── tests/                      # Unit & integration tests
│── scripts/                    # Utility scripts (DB migration, testing, etc.)
│── Dockerfile                  # Docker configuration
│── docker-compose.yml          # Docker Compose for multi-service setup
│── .env                        # Environment variables
│── requirements.txt            # Python dependencies
│── main.py                     # FastAPI entry point
│── README.md                   # Project documentation


uvicorn main:app --reload


1.save selected data into mongodb after cleaning process complete
2.
3.
4.analyze sentiment using data from kafka topic by using bert/bart model
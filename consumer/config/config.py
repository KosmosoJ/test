import os
from dotenv import load_dotenv, find_dotenv


if find_dotenv():
    load_dotenv()
    KAFKA_URL = os.getenv("KAFKA_URL")
    KAFKA_USER = os.getenv("KAFKA_USER")
    KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
    MONGO_HOST = os.getenv("MONGO_HOST")
    MONGO_USER = os.getenv("MONGO_USER")
    MONGO_PASSWORD = os.getenv("MONGO_PASS")
    MONGO_DB = os.getenv("MONGO_DB")

    KAFKA_PRODUCER_CONF = {
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": KAFKA_USER,
        "sasl_plain_password": KAFKA_PASSWORD,
    }

    KAFKA_CONSUMER_CONF = {
        "security_protocol": "SASL_PLAINTEXT",
        "enable_auto_commit": True,
        "auto_offset_reset": "earliest",
        "session_timeout_ms": 10000,
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": KAFKA_USER,
        "sasl_plain_password": KAFKA_PASSWORD,
    }

else:
    exit(".env not found")

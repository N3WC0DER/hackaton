import datetime
import time
import json
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = 'stt-queue'

def get_kafka_producer():
    retries = 5
    while retries > 0:
        try:
            return KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}, retrying...")
            retries -= 1
            time.sleep(10)
    raise Exception("Could not connect to Kafka after retries")

# Инициализация после проверки зависимостей
producer = get_kafka_producer()

def send_to_kafka():
    """Отправка сообщения в Kafka"""
    message = {
        'counter': 0
    }
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()


def main():
    """Основной цикл"""
    while True:
        print("Processing completed. Waiting for 1 hours...")
        send_to_kafka()
        time.sleep(60 * 60) # раз в час

if __name__ == "__main__":
    main()

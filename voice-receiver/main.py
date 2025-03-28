import json
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = 'stt-queue'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
        print(f"Starting call processing at {datetime.now()}")
        print("Processing completed. Waiting for 1 hours...")
        send_to_kafka()
        time.sleep(60 * 60) # раз в час

if __name__ == "__main__":
    main()
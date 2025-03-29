import time
import json
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import os
from urllib.parse import urlparse

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
        region_name=os.getenv('S3_REGION_NAME'),
        config=Config(signature_version='s3v4')
    )

def upload_to_vk_cloud_s3(file_path, call_id):
    s3_client = get_s3_client()
    object_name = f"{call_id}.mp3"
    
    try:
        s3_client.upload_file(
            file_path,
            os.getenv('S3_BUCKET_NAME'),
            object_name,
            ExtraArgs={'ACL': 'private'}
        )
        return f"{os.getenv('S3_ENDPOINT_URL')}/{os.getenv('S3_BUCKET_NAME')}/{object_name}"
    except Exception as e:
        print(f"Error uploading to VK Cloud S3: {e}")
        raise

load_dotenv()

# KAFKA_TOPIC = 'stt-queue'

# def get_kafka_producer():
#     retries = 5
#     while retries > 0:
#         try:
#             return KafkaProducer(
#                 bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(','),
#                 value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#             )
#         except Exception as e:
#             print(f"Failed to connect to Kafka: {e}, retrying...")
#             retries -= 1
#             time.sleep(10)
#     raise Exception("Could not connect to Kafka after retries")

# # Инициализация после проверки зависимостей
# producer = get_kafka_producer()

# def send_to_kafka():
#     """Отправка сообщения в Kafka"""
#     message = {
#         'counter': 0
#     }
#     producer.send(KAFKA_TOPIC, value=message)
#     producer.flush()


def main():
    """Основной цикл"""
    upload_to_vk_cloud_s3("./call_records/1.mp3", 1)
    # while True:
    #     print("Processing completed. Waiting for 1 hours...")
    #     send_to_kafka()
    #     time.sleep(60 * 60) # раз в час

if __name__ == "__main__":
    main()

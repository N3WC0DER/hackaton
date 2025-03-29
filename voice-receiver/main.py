from datetime import datetime
import json
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import os
from urllib.parse import urlparse
import psycopg2
from pydub import AudioSegment
import os
import wave
import time

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

PROFILE = os.getenv('PROFILE')

if PROFILE == "bitrix":
    BITRIX_API_URL = os.getenv("BITRIX_API_URL")
    BITRIX_WEBHOOK_TOKEN = os.getenv("BITRIX_WEBHOOK_TOKEN")
elif PROFILE == "local":
    CALLS_FOLDER = os.getenv("CALLS_FOLDER")

INTERVAL = int(os.getenv('INTERVAL'))

def get_bitrix_calls():
    """Получение записей звонков из Битрикс"""
    
    if PROFILE != "bitrix":
        raise Exception("Change profile")

    url = f"{BITRIX_API_URL}/{BITRIX_WEBHOOK_TOKEN}/voximplant.statistic.get"
    
    # Получаем звонки за последние 24 часа
    date_from = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    date_to = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    params = {
        'ORDER': {'DATE_CREATE': 'DESC'},
        'FILTER': {
            '>=DATE_CREATE': date_from,
            '<=DATE_CREATE': date_to
        }
    }
    
    response = requests.post(url, json=params)
    response.raise_for_status()
    
    return response.json().get('result', [])

def get_local_calls():
    extensions = ['.mp3']
    audio_files = []
    
    for root, dirs, files in os.walk(CALLS_FOLDER):
        for file in files:
            if any(file.lower().endswith(ext) for ext in extensions):
                audio_files.append(os.path.join(root, file))
    
    return audio_files

def convert_mp3_to_wav(input_mp3, output_wav=None):
    """
    Конвертирует MP3 файл в WAV формат.

    :param input_mp3: Путь к входному MP3 файлу.
    :param output_wav: Путь к выходному WAV файлу. Если не указан, будет создан в той же папке.
    """
    # Загружаем MP3 фа
    audio = AudioSegment.from_mp3(input_mp3)

    # Если выходной путь не указан, заменяем расширение на .wav
    if output_wav is None:
        base_name = os.path.splitext(input_mp3)[0]
        output_wav = f"{base_name}.wav"

    # Экспортируем в WAV
    audio.export(output_wav, format="wav")
    print(f"Файл успешно сконвертирован: {output_wav}")

def get_audio_duration_wav(file_path):
    with wave.open(file_path, 'rb') as audio_file:
        frames = audio_file.getnframes()
        rate = audio_file.getframerate()
        duration = frames / float(rate)
        return duration


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def save_call_to_db(call_id, record_url, duration, created_at):
    """Сохранение информации о звонке в БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    INSERT INTO call_records (call_id, record_url, duration, created_at)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (call_id) DO UPDATE
    SET record_url = EXCLUDED.record_url
    """
    
    cursor.execute(query, (call_id, record_url, duration, created_at))
    conn.commit()
    cursor.close()
    conn.close()

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
        region_name=os.getenv('S3_REGION_NAME')
    )

def upload_to_s3(file_path, call_id):
    """Загружает файл в VK Cloud S3 с правильными заголовками"""
    s3_client = get_s3_client()
    object_name = f"call_records/{call_id}.wav"

    try:
        with open(file_path, 'rb') as data:
            s3_client.put_object(
                Bucket='voicemetrics',
                Key=object_name,
                Body=data,
                ChecksumSHA256='UNSIGNED_PAYLOAD'
            )
        return f"{os.getenv('S3_ENDPOINT_URL')}/{os.getenv('S3_BUCKET_NAME')}/{object_name}"
    except Exception as e:
        print(f"Error uploading to VK Cloud S3: {str(e)}")
        raise

def check_s3_connection():
    s3_client = get_s3_client()
    try:
        s3_client.list_buckets()
        print("Successfully connected to VK Cloud S3")
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

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

def send_to_kafka(call_id):
    """Отправка сообщения в Kafka"""
    message = {
        'call_id': call_id
    }
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()

def process_bitrix_calls():
    """Основная функция обработки звонков"""
    
    if PROFILE != "bitrix":
        raise Exception("Change PROFILE")

    try:
        calls = get_bitrix_calls()
        
        for call in calls:
            call_id = call['ID']
            record_url = call['RECORD_URL']
            call_datetime = call['CALL_START_DATE']

            if record_url:
                # Скачиваем файл записи
                local_filename = f"/tmp/{call_id}.mp3"
                with requests.get(record_url, stream=True) as r:
                    r.raise_for_status()
                    with open(local_filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
               
                # Конвертируем .mp3 в .wav
                convert_mp3_to_wav(local_filename)
                os.remove(local_filename)
                local_filename = local_filename.replace(".mp3", ".wav")

                # Загружаем на S3
                s3_url = upload_to_s3(local_filename, call_id)
                
                # Сохраняем в БД
                save_call_to_db(call_id, s3_url, get_audio_duration_wav(local_filename), call_datetime)
                
                # Отправляем в Kafka
                send_to_kafka(call_id)
                
                # Удаляем временный файл
                os.remove(local_filename)
                
                print(f"Processed call {call_id}")
    
    except Exception as e:
        print(f"Error processing calls: {e}")

def process_local_calls():
    if PROFILE != "local":
        raise Exception("Change PROFILE")

    try:
        calls_path = get_local_calls()

        for call_path in calls_path:
            filename_with_ext = os.path.basename(call_path)  # 'song_name.mp3'

            # Разделяем имя и расширение
            call_id = os.path.splitext(filename_with_ext)[0]

            # Конвертируем .mp3 в .wav
            convert_mp3_to_wav(call_path)
            #os.remove(call_path)
            call_path = call_path.replace(".mp3", ".wav")

            # Загружаем на S3
            s3_url = upload_to_s3(call_path, call_id)

            # Сохраняем в БД
            save_call_to_db(call_id, s3_url, get_audio_duration_wav(call_path), datetime.now())

            # Отправляем в Kafka
            send_to_kafka(call_id)

            # Удаляем временный файл
            os.remove(call_path)

            print(f"Processed call {call_id}")

    except Exception as e:
        print(f"Error processing calls: {e}")

def main():
    """Основной цикл"""
    if not check_s3_connection():
        raise Exception("Cannot connect to VK Cloud S3")
    
    #while True:
    print(f"Starting call processing at {datetime.now()}")
    if PROFILE == "bitrix":
            process_bitrix_calls()
    elif PROFILE == "local":
        process_local_calls()
    print(f"Processing completed. Waiting for {INTERVAL} seconds...")

        #time.sleep(INTERVAL)

if __name__ == "__main__":
    main()

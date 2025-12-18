import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import boto3
from io import BytesIO
import os
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Настройки подключения из переменных окружения
PG_CONNECTION_STRING = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET')
LAST_EXTRACTION_FILE = 'last_extraction.txt'

# Инициализация клиентов
engine = create_engine(PG_CONNECTION_STRING)
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=boto3.session.Config(signature_version='s3v4')
)

def create_bucket():
    """Создает бакет в MinIO если он не существует"""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket {BUCKET_NAME} already exists")
    except:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket {BUCKET_NAME} created")

def get_last_extraction_time(table_name):
    """Получает время последнего извлечения для таблицы"""
    try:
        key = f"metadata/{table_name}_{LAST_EXTRACTION_FILE}"
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        return response['Body'].read().decode('utf-8').strip()
    except:
        return None

def save_last_extraction_time(table_name, timestamp):
    """Сохраняет время последнего извлечения для таблицы"""
    key = f"metadata/{table_name}_{LAST_EXTRACTION_FILE}"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=timestamp.encode('utf-8')
    )

def upload_to_s3(df, table_name, extraction_type):
    """Загружает DataFrame в S3 в формате Parquet"""
    try:
        # Создаем буфер для Parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        
        # Формируем ключ для S3
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if extraction_type == 'full':
            key = f"full/{table_name}/{table_name}_{timestamp}.parquet"
        else:
            key = f"incremental/{table_name}/{table_name}_{timestamp}.parquet"
        
        # Загружаем в S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=parquet_buffer.getvalue()
        )
        
        logger.info(f"Successfully uploaded {table_name} to S3: {key}")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading {table_name} to S3: {str(e)}")
        return False

def extract_full_table(table_name, query=None):
    """Полная перезагрузка таблицы"""
    try:
        if query is None:
            query = f"SELECT * FROM {table_name}"
        
        df = pd.read_sql(query, engine)
        success = upload_to_s3(df, table_name, 'full')
        
        if success:
            logger.info(f"Full extraction completed for {table_name}, rows: {len(df)}")
        return success
        
    except Exception as e:
        logger.error(f"Error in full extraction for {table_name}: {str(e)}")
        return False

def extract_incremental_table(table_name, timestamp_column='updated_at'):
    """Инкрементальное извлечение таблицы"""
    try:
        last_extraction = get_last_extraction_time(table_name)
        
        if last_extraction:
            query = f"""
            SELECT * FROM {table_name} 
            WHERE {timestamp_column} > '{last_extraction}'
            """
        else:
            # Первое извлечение - берем все данные
            query = f"SELECT * FROM {table_name}"
        
        df = pd.read_sql(query, engine)
        
        if not df.empty:
            success = upload_to_s3(df, table_name, 'incremental')
            if success:
                # Сохраняем максимальное время обновления как время последнего извлечения
                max_timestamp = df[timestamp_column].max()
                if pd.notna(max_timestamp):
                    save_last_extraction_time(table_name, str(max_timestamp))
                
                logger.info(f"Incremental extraction completed for {table_name}, rows: {len(df)}")
            return success
        else:
            logger.info(f"No new data for {table_name}")
            return True
            
    except Exception as e:
        logger.error(f"Error in incremental extraction for {table_name}: {str(e)}")
        return False

def extract_users():
    """Извлечение пользователей (инкрементально)"""
    return extract_incremental_table('users')

def extract_students():
    """Извлечение студентов (инкрементально)"""
    return extract_incremental_table('students')

def extract_teachers():
    """Извлечение преподавателей (инкрементально)"""
    return extract_incremental_table('teachers')

def extract_subjects():
    """Извлечение предметов (полная перезагрузка)"""
    return extract_full_table('subjects')

def extract_teacher_subjects():
    """Извлечение связей преподаватель-предмет (инкрементально)"""
    return extract_incremental_table('teacher_subjects')

def extract_lesson_packs():
    """Извлечение пакетов уроков (полная перезагрузка)"""
    return extract_full_table('lesson_packs')

def extract_students_purchases():
    """Извлечение приобретенных пакетов (инкрементально)"""
    return extract_incremental_table('students_purchases')

def extract_lessons():
    """Извлечение уроков (инкрементально)"""
    return extract_incremental_table('lessons')

def extract_homeworks():
    """Извлечение домашних заданий (инкрементально)"""
    return extract_incremental_table('homeworks')

def run_all_extractions():
    """Запуск всех извлечений"""
    logger.info("Starting data extraction process...")
    
    # Создаем бакет если не существует
    create_bucket()
    
    extraction_results = {}
    
    # Полные перезагрузки
    extraction_results['subjects'] = extract_subjects()
    extraction_results['lesson_packs'] = extract_lesson_packs()
    
    # Инкрементальные извлечения
    extraction_results['users'] = extract_users()
    extraction_results['students'] = extract_students()
    extraction_results['teachers'] = extract_teachers()
    extraction_results['teacher_subjects'] = extract_teacher_subjects()
    extraction_results['students_purchases'] = extract_students_purchases()
    extraction_results['lessons'] = extract_lessons()
    extraction_results['homeworks'] = extract_homeworks()
    
    # Логирование результатов
    successful = sum(extraction_results.values())
    total = len(extraction_results)
    
    logger.info(f"Extraction completed: {successful}/{total} successful")
    
    for table, success in extraction_results.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"  {table}: {status}")
    
    return extraction_results

def main():
    run_all_extractions()

if __name__ == "__main__":
    main()

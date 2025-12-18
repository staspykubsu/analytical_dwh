import pandas as pd
import boto3
from io import BytesIO
import logging
from datetime import datetime, date
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickhouseError
import numpy as np
from decimal import Decimal
import os
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Настройки подключения
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET')

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')

# Инициализация клиентов
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=boto3.session.Config(signature_version='s3v4')
)

clickhouse_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)

def list_s3_files(prefix):
    """Получает список файлов в S3 по префиксу"""
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    files.append(obj['Key'])
        return sorted(files)
    except Exception as e:
        logger.error(f"Error listing S3 files with prefix {prefix}: {str(e)}")
        return []

def read_parquet_from_s3(key):
    """Читает Parquet файл из S3 в DataFrame"""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        logger.info(f"Read {len(df)} rows from {key}")
        return df
    except Exception as e:
        logger.error(f"Error reading {key} from S3: {str(e)}")
        return pd.DataFrame()

def get_latest_subject_file():
    """Получает последний файл subjects из папки full"""
    files = list_s3_files('full/subjects/')
    if not files:
        logger.error("No subject files found in full/subjects/")
        return None
    return files[-1]

def execute_clickhouse_query(query, params=None):
    """Выполняет запрос в ClickHouse"""
    try:
        clickhouse_client.execute(query, params)
        logger.info(f"Query executed successfully: {query[:100]}...")
        return True
    except ClickhouseError as e:
        logger.error(f"ClickHouse query error: {str(e)}")
        return False

def safe_int(value, default=0):
    """Безопасное преобразование в integer"""
    try:
        if pd.isna(value):
            return default
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_float(value, default=0.0):
    """Безопасное преобразование в float"""
    try:
        if pd.isna(value):
            return default
        if isinstance(value, Decimal):
            return float(value)
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_decimal(value, default=Decimal('0.0')):
    """Безопасное преобразование в Decimal"""
    try:
        if pd.isna(value):
            return default
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))
    except:
        return default

def safe_str(value, default=''):
    """Безопасное преобразование в string"""
    try:
        if pd.isna(value):
            return default
        return str(value)
    except (ValueError, TypeError):
        return default

def safe_date_key(date_value):
    """Безопасное преобразование даты в date_key"""
    try:
        if pd.isna(date_value):
            return 0
        return int(pd.to_datetime(date_value).strftime('%Y%m%d'))
    except:
        return 0

def safe_datetime_to_date(datetime_value):
    """Безопасное преобразование datetime в date"""
    try:
        if pd.isna(datetime_value):
            return None
        return pd.to_datetime(datetime_value).date()
    except:
        return None

def safe_datetime(datetime_value):
    """Безопасное преобразование в datetime"""
    try:
        if pd.isna(datetime_value):
            return None
        return pd.to_datetime(datetime_value)
    except:
        return None

def load_dim_subject():
    """Загрузка Dim_Subject - полная перезагрузка"""
    logger.info("Starting Dim_Subject load...")
    
    # Очистка таблицы
    if not execute_clickhouse_query("TRUNCATE TABLE Dim_Subject"):
        return False
    
    # Получение последнего файла subjects
    subject_file = get_latest_subject_file()
    if not subject_file:
        return False
    
    # Чтение данных
    df = read_parquet_from_s3(subject_file)
    if df.empty:
        logger.warning("No data found for Dim_Subject")
        return True
    
    # Подготовка данных
    for _, row in df.iterrows():
        query = """
        INSERT INTO Dim_Subject (subject_sk, subject_id_nk, subject_name)
        VALUES (%(subject_sk)s, %(subject_id_nk)s, %(subject_name)s)
        """
        params = {
            'subject_sk': safe_int(row.get('subject_id')),
            'subject_id_nk': safe_int(row.get('subject_id')),
            'subject_name': safe_str(row.get('name'))
        }
        if not execute_clickhouse_query(query, params):
            return False
    
    logger.info(f"Dim_Subject loaded: {len(df)} rows")
    return True

def load_fact_homeworks():
    """Загрузка Fact_Homeworks - полная перезагрузка"""
    logger.info("Starting Fact_Homeworks load...")
    
    # Очистка таблицы
    if not execute_clickhouse_query("TRUNCATE TABLE Fact_Homeworks"):
        return False
    
    # Получение всех файлов homeworks
    files = list_s3_files("incremental/homeworks/")
    if not files:
        logger.warning("No files found for Fact_Homeworks")
        return True
    
    total_rows = 0
    
    for file_key in files:
        df = read_parquet_from_s3(file_key)
        if df.empty:
            continue
        
        total_rows += len(df)
        
        # Подготовка данных для каждой записи
        for _, row in df.iterrows():
            # Получаем lesson_id для поиска student_id и subject_id
            lesson_id = safe_int(row.get('lesson_id'))
            
            # Ищем соответствующий lesson в папке incremental/lessons
            student_sk = 0
            subject_sk = 0
            
            # Поиск в файлах lessons
            lesson_files = list_s3_files("incremental/lessons/")
            for lesson_file in lesson_files:
                lessons_df = read_parquet_from_s3(lesson_file)
                if not lessons_df.empty and 'lesson_id' in lessons_df.columns:
                    lesson_match = lessons_df[lessons_df['lesson_id'] == lesson_id]
                    if not lesson_match.empty:
                        # Получаем student_id и teacher_subject_id
                        student_sk = safe_int(lesson_match.iloc[0].get('student_id'))
                        
                        # Получаем teacher_subject_id и ищем subject_id
                        teacher_subject_id = safe_int(lesson_match.iloc[0].get('teacher_subject_id'))
                        
                        # Ищем subject_id в teacher_subjects
                        teacher_subject_files = list_s3_files("incremental/teacher_subjects/")
                        for ts_file in teacher_subject_files:
                            ts_df = read_parquet_from_s3(ts_file)
                            if not ts_df.empty and 'teacher_subject_id' in ts_df.columns:
                                ts_match = ts_df[ts_df['teacher_subject_id'] == teacher_subject_id]
                                if not ts_match.empty:
                                    subject_sk = safe_int(ts_match.iloc[0].get('subject_id'))
                                    break
                        break
            
            # Получаем updated_at из данных
            updated_at = safe_datetime(row.get('updated_at'))
            
            # Подготовка параметров для вставки (БЕЗ days_overdue)
            query = """
            INSERT INTO Fact_Homeworks (
                homework_fact_id, date_assigned_key, date_deadline_key,
                date_submitted_key, student_sk, subject_sk, homework_id_nk,
                score, homework_status, updated_at
            ) VALUES (
                %(homework_fact_id)s, %(date_assigned_key)s, %(date_deadline_key)s,
                %(date_submitted_key)s, %(student_sk)s, %(subject_sk)s, %(homework_id_nk)s,
                %(score)s, %(homework_status)s, %(updated_at)s
            )
            """
            
            params = {
                'homework_fact_id': safe_int(row.get('homework_id')),
                'date_assigned_key': safe_date_key(row.get('created_at')),
                'date_deadline_key': safe_date_key(row.get('deadline')),
                'date_submitted_key': safe_date_key(row.get('submitted_at')) if pd.notna(row.get('submitted_at')) else None,
                'student_sk': student_sk,
                'subject_sk': subject_sk,
                'homework_id_nk': safe_int(row.get('homework_id')),
                'score': safe_int(row.get('score')) if pd.notna(row.get('score')) else None,
                'homework_status': safe_str(row.get('status', 'assigned')),
                'updated_at': updated_at
            }
            
            if not execute_clickhouse_query(query, params):
                return False
    
    logger.info(f"Fact_Homeworks loaded: {total_rows} rows from {len(files)} files")
    return True

def load_fact_lessons():
    """Загрузка Fact_Lessons - полная перезагрузка"""
    logger.info("Starting Fact_Lessons load...")
    
    # Очистка таблицы
    if not execute_clickhouse_query("TRUNCATE TABLE Fact_Lessons"):
        return False
    
    # Получение всех файлов lessons
    files = list_s3_files("incremental/lessons/")
    if not files:
        logger.warning("No files found for Fact_Lessons")
        return True
    
    total_rows = 0
    
    # ЗАГРУЖАЕМ ВСЕ ДАННЫЕ О ПРЕПОДАВАТЕЛЯХ ИЗ S3 И НАХОДИМ ПОСЛЕДНИЕ ЗАПИСИ ДЛЯ КАЖДОГО teacher_id
    all_teachers_data = pd.DataFrame()
    teacher_files = list_s3_files("incremental/teachers/")
    for teacher_file in teacher_files:
        df = read_parquet_from_s3(teacher_file)
        if not df.empty:
            # Преобразуем updated_at в datetime для сортировки
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])
            all_teachers_data = pd.concat([all_teachers_data, df], ignore_index=True)
    
    # ГРУППИРУЕМ ПО teacher_id И БЕРЕМ ПОСЛЕДНЮЮ ЗАПИСЬ (С МАКСИМАЛЬНЫМ updated_at)
    latest_teachers = pd.DataFrame()
    if not all_teachers_data.empty:
        # Сортируем по updated_at (по убыванию) и берем первую запись для каждого teacher_id
        all_teachers_data = all_teachers_data.sort_values('updated_at', ascending=False)
        latest_teachers = all_teachers_data.drop_duplicates(subset=['teacher_id'], keep='first')
        logger.info(f"Loaded {len(latest_teachers)} latest teacher records from S3")
    
    for file_key in files:
        df = read_parquet_from_s3(file_key)
        if df.empty:
            continue
        
        total_rows += len(df)
        
        # Подготовка данных для каждой записи
        for _, row in df.iterrows():
            # Получаем teacher_subject_id для поиска teacher_id и subject_id
            teacher_subject_id = safe_int(row.get('teacher_subject_id'))
            
            # Ищем teacher_subject в папке incremental/teacher_subjects
            teacher_sk = 0
            subject_sk = 0
            
            # Получаем teacher_id из teacher_subjects
            teacher_subject_files = list_s3_files("incremental/teacher_subjects/")
            teacher_found = False
            
            for ts_file in teacher_subject_files:
                ts_df = read_parquet_from_s3(ts_file)
                if not ts_df.empty and 'teacher_subject_id' in ts_df.columns:
                    ts_match = ts_df[ts_df['teacher_subject_id'] == teacher_subject_id]
                    if not ts_match.empty:
                        teacher_sk = safe_int(ts_match.iloc[0].get('teacher_id'))
                        subject_sk = safe_int(ts_match.iloc[0].get('subject_id'))
                        teacher_found = True
                        break
            
            # Если не нашли через teacher_subjects, проверяем есть ли teacher_id прямо в уроке
            if not teacher_found and 'teacher_id' in row:
                teacher_sk = safe_int(row.get('teacher_id'))
            
            # ПОЛУЧАЕМ hourly_rate ПРЕПОДАВАТЕЛЯ ИЗ ПОСЛЕДНЕЙ ЗАПИСИ В ДАННЫХ S3
            teacher_cost_amount = Decimal('0.0')
            if teacher_sk > 0 and not latest_teachers.empty:
                teacher_match = latest_teachers[latest_teachers['teacher_id'] == teacher_sk]
                if not teacher_match.empty:
                    teacher_cost_amount = safe_decimal(teacher_match.iloc[0].get('hourly_rate', Decimal('0.0')))
                    logger.info(f"Teacher {teacher_sk} latest hourly rate: {teacher_cost_amount}")
                else:
                    logger.warning(f"Teacher {teacher_sk} not found in loaded teacher data")
            
            # Вычисляем duration_minutes
            duration_minutes = 60  # значение по умолчанию
            if pd.notna(row.get('scheduled_start_time')) and pd.notna(row.get('scheduled_end_time')):
                try:
                    start_time = pd.to_datetime(row['scheduled_start_time'])
                    end_time = pd.to_datetime(row['scheduled_end_time'])
                    duration_minutes = int((end_time - start_time).total_seconds() / 60)
                except:
                    duration_minutes = 60
            
            # Получаем updated_at из данных
            updated_at = safe_datetime(row.get('updated_at'))
            
            # Подготовка параметров для вставки
            query = """
            INSERT INTO Fact_Lessons (
                lesson_fact_id, date_key, time_start, student_sk,
                teacher_sk, subject_sk, lesson_id_nk, duration_minutes,
                teacher_cost_amount, lesson_status, updated_at
            ) VALUES (
                %(lesson_fact_id)s, %(date_key)s, %(time_start)s, %(student_sk)s,
                %(teacher_sk)s, %(subject_sk)s, %(lesson_id_nk)s, %(duration_minutes)s,
                %(teacher_cost_amount)s, %(lesson_status)s, %(updated_at)s
            )
            """
            
            params = {
                'lesson_fact_id': safe_int(row.get('lesson_id')),
                'date_key': safe_date_key(row.get('scheduled_start_time')),
                'time_start': row.get('scheduled_start_time') if pd.notna(row.get('scheduled_start_time')) else None,
                'student_sk': safe_int(row.get('student_id')),
                'teacher_sk': teacher_sk,
                'subject_sk': subject_sk,
                'lesson_id_nk': safe_int(row.get('lesson_id')),
                'duration_minutes': duration_minutes,
                'teacher_cost_amount': teacher_cost_amount,
                'lesson_status': safe_str(row.get('status', 'scheduled')),
                'updated_at': updated_at
            }
            
            if not execute_clickhouse_query(query, params):
                return False
    
    logger.info(f"Fact_Lessons loaded: {total_rows} rows from {len(files)} files")
    return True

def load_fact_sales():
    """Загрузка Fact_Sales - полная перезагрузка"""
    logger.info("Starting Fact_Sales load...")
    
    # Очистка таблицы
    if not execute_clickhouse_query("TRUNCATE TABLE Fact_Sales"):
        return False
    
    # Получение всех файлов students_purchases
    files = list_s3_files("incremental/students_purchases/")
    if not files:
        logger.warning("No files found for Fact_Sales")
        return True
    
    total_rows = 0
    
    for file_key in files:
        df = read_parquet_from_s3(file_key)
        if df.empty:
            continue
        
        total_rows += len(df)
        
        # Подготовка данных для каждой записи
        for _, row in df.iterrows():
            # Получаем updated_at из данных
            updated_at = safe_datetime(row.get('updated_at'))
            
            query = """
            INSERT INTO Fact_Sales (
                sales_id, date_key, student_sk, purchase_id_nk,
                purchase_amount, lessons_total, purchase_status, updated_at
            ) VALUES (
                %(sales_id)s, %(date_key)s, %(student_sk)s, %(purchase_id_nk)s,
                %(purchase_amount)s, %(lessons_total)s, %(purchase_status)s, %(updated_at)s
            )
            """
            
            params = {
                'sales_id': safe_int(row.get('purchase_id')),
                'date_key': safe_date_key(row.get('purchase_date')),
                'student_sk': safe_int(row.get('student_id')),
                'purchase_id_nk': safe_int(row.get('purchase_id')),
                'purchase_amount': safe_decimal(row.get('purchase_price', Decimal('0.0'))),
                'lessons_total': safe_int(row.get('lessons_total', 0)),
                'purchase_status': safe_str(row.get('status', 'active')),
                'updated_at': updated_at
            }
            
            if not execute_clickhouse_query(query, params):
                return False
    
    logger.info(f"Fact_Sales loaded: {total_rows} rows from {len(files)} files")
    return True

def get_table_structure(table_name):
    """Получает структуру таблицы из ClickHouse"""
    try:
        query = f"DESCRIBE TABLE {table_name}"
        result = clickhouse_client.execute(query)
        columns = [row[0] for row in result]
        logger.info(f"Table {table_name} structure: {columns}")
        return columns
    except Exception as e:
        logger.error(f"Error getting structure of {table_name}: {str(e)}")
        return []

def load_dim_student():
    """Загрузка Dim_Student - SCD2 с использованием updated_at как valid_from"""
    logger.info("Starting Dim_Student SCD2 load...")
    
    # Получаем структуру таблицы
    student_columns = get_table_structure('Dim_Student')
    has_user_id_nk = 'user_id_nk' in student_columns
    
    # Получение всех файлов students
    files = list_s3_files("incremental/students/")
    if not files:
        logger.warning("No files found for Dim_Student")
        return True
    
    # Чтение всех данных students с учетом updated_at
    all_students = pd.DataFrame()
    for file_key in files:
        df = read_parquet_from_s3(file_key)
        if not df.empty:
            # Преобразуем updated_at в datetime
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])
            all_students = pd.concat([all_students, df], ignore_index=True)
    
    if all_students.empty:
        logger.warning("No student data found")
        return True
    
    # Получение всех файлов users
    user_files = list_s3_files("incremental/users/")
    all_users = pd.DataFrame()
    for file_key in user_files:
        df = read_parquet_from_s3(file_key)
        if not df.empty:
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])
            all_users = pd.concat([all_users, df], ignore_index=True)
    
    # Объединение данных students и users
    if not all_users.empty:
        all_students = all_students.merge(
            all_users[['user_id', 'first_name', 'last_name', 'phone_number', 'status', 'updated_at']],
            left_on='user_id',
            right_on='user_id',
            how='left',
            suffixes=('_student', '_user')
        )
        
        # Берем максимальный updated_at из student и user как valid_from
        all_students['valid_from'] = all_students[['updated_at_student', 'updated_at_user']].max(axis=1)
    
    # Находим максимальный updated_at для каждого student_id
    if not all_students.empty:
        max_updated_by_id = all_students.groupby('student_id')['valid_from'].max().reset_index()
        max_updated_by_id.columns = ['student_id', 'max_updated_at']
        
        # Объединяем обратно, чтобы знать для каждой записи, является ли она последней
        all_students = all_students.merge(
            max_updated_by_id,
            on='student_id',
            how='left'
        )
        
        # Определяем is_current: 1 если valid_from равен максимальному updated_at для этого student_id
        all_students['is_current'] = (all_students['valid_from'] == all_students['max_updated_at']).astype(int)
    
    future_date = datetime(2099, 12, 31).date()
    inserted_count = 0
    
    # Сначала очищаем таблицу (или можно делать upsert, но проще очистить и загрузить заново для SCD2)
    if not execute_clickhouse_query("TRUNCATE TABLE Dim_Student"):
        return False
    
    for _, row in all_students.iterrows():
        student_id = safe_int(row.get('student_id'))
        full_name = f"{safe_str(row.get('first_name'))} {safe_str(row.get('last_name'))}".strip()
        phone_number = safe_str(row.get('phone_number', ''))
        current_grade = safe_str(row.get('current_grade', ''))
        status = safe_str(row.get('status', 'active'))
        valid_from = safe_datetime_to_date(row.get('valid_from'))
        is_current = safe_int(row.get('is_current', 0))
        updated_at = safe_datetime(row.get('valid_from'))  # используем valid_from как updated_at для таблицы
        
        if valid_from is None:
            valid_from = datetime.now().date()
        
        # Вычисляем valid_to
        if is_current == 1:
            valid_to = future_date
        else:
            # Находим следующую запись с тем же student_id для определения valid_to
            same_id_records = all_students[all_students['student_id'] == student_id].copy()
            same_id_records = same_id_records.sort_values('valid_from')
            
            current_idx = same_id_records[same_id_records['valid_from'] == row['valid_from']].index
            if not current_idx.empty:
                current_idx = current_idx[0]
                next_records = same_id_records[same_id_records.index > current_idx]
                if not next_records.empty:
                    # valid_to = следующий valid_from - 1 день
                    next_valid_from = next_records.iloc[0]['valid_from']
                    valid_to = next_valid_from.date() - pd.Timedelta(days=1)
                else:
                    valid_to = future_date
            else:
                valid_to = future_date
        
        # Вставляем запись
        if has_user_id_nk:
            insert_query = """
            INSERT INTO Dim_Student (
                student_sk, student_id_nk, user_id_nk, full_name,
                phone_number, current_grade, status, 
                valid_from, valid_to, is_current, updated_at
            ) VALUES (
                %(sk)s, %(nk)s, %(user_nk)s, %(full_name)s,
                %(phone_number)s, %(current_grade)s, %(status)s, 
                %(valid_from)s, %(valid_to)s, %(is_current)s, %(updated_at)s
            )
            """
            insert_params = {
                'sk': student_id,
                'nk': student_id,
                'user_nk': safe_int(row.get('user_id')),
                'full_name': full_name,
                'phone_number': phone_number,
                'current_grade': current_grade,
                'status': status,
                'valid_from': valid_from,
                'valid_to': valid_to,
                'is_current': is_current,
                'updated_at': updated_at
            }
        else:
            insert_query = """
            INSERT INTO Dim_Student (
                student_sk, student_id_nk, full_name,
                phone_number, current_grade, status, 
                valid_from, valid_to, is_current, updated_at
            ) VALUES (
                %(sk)s, %(nk)s, %(full_name)s,
                %(phone_number)s, %(current_grade)s, %(status)s, 
                %(valid_from)s, %(valid_to)s, %(is_current)s, %(updated_at)s
            )
            """
            insert_params = {
                'sk': student_id,
                'nk': student_id,
                'full_name': full_name,
                'phone_number': phone_number,
                'current_grade': current_grade,
                'status': status,
                'valid_from': valid_from,
                'valid_to': valid_to,
                'is_current': is_current,
                'updated_at': updated_at
            }
        
        if not execute_clickhouse_query(insert_query, insert_params):
            return False
        inserted_count += 1
    
    logger.info(f"Dim_Student SCD2 load completed: {inserted_count} rows inserted")
    return True

def load_dim_teacher():
    """Загрузка Dim_Teacher - SCD2 с использованием updated_at как valid_from"""
    logger.info("Starting Dim_Teacher SCD2 load...")
    
    # Получаем структуру таблицы
    teacher_columns = get_table_structure('Dim_Teacher')
    has_user_id_nk = 'user_id_nk' in teacher_columns
    
    # Получение всех файлов teachers
    files = list_s3_files("incremental/teachers/")
    if not files:
        logger.warning("No files found for Dim_Teacher")
        return True
    
    # Чтение всех данных teachers с учетом updated_at
    all_teachers = pd.DataFrame()
    for file_key in files:
        df = read_parquet_from_s3(file_key)
        if not df.empty:
            # Преобразуем updated_at в datetime
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])
            all_teachers = pd.concat([all_teachers, df], ignore_index=True)
    
    if all_teachers.empty:
        logger.warning("No teacher data found")
        return True
    
    # Получение всех файлов users
    user_files = list_s3_files("incremental/users/")
    all_users = pd.DataFrame()
    for file_key in user_files:
        df = read_parquet_from_s3(file_key)
        if not df.empty:
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])
            all_users = pd.concat([all_users, df], ignore_index=True)
    
    # Объединение данных teachers и users
    if not all_users.empty:
        all_teachers = all_teachers.merge(
            all_users[['user_id', 'first_name', 'last_name', 'phone_number', 'status', 'updated_at']],
            left_on='user_id',
            right_on='user_id',
            how='left',
            suffixes=('_teacher', '_user')
        )
        
        # Берем максимальный updated_at из teacher и user как valid_from
        all_teachers['valid_from'] = all_teachers[['updated_at_teacher', 'updated_at_user']].max(axis=1)
    
    # Находим максимальный updated_at для каждого teacher_id
    if not all_teachers.empty:
        max_updated_by_id = all_teachers.groupby('teacher_id')['valid_from'].max().reset_index()
        max_updated_by_id.columns = ['teacher_id', 'max_updated_at']
        
        # Объединяем обратно, чтобы знать для каждой записи, является ли она последней
        all_teachers = all_teachers.merge(
            max_updated_by_id,
            on='teacher_id',
            how='left'
        )
        
        # Определяем is_current: 1 если valid_from равен максимальному updated_at для этого teacher_id
        all_teachers['is_current'] = (all_teachers['valid_from'] == all_teachers['max_updated_at']).astype(int)
    
    future_date = datetime(2099, 12, 31).date()
    inserted_count = 0
    
    # Сначала очищаем таблицу (или можно делать upsert, но проще очистить и загрузить заново для SCD2)
    if not execute_clickhouse_query("TRUNCATE TABLE Dim_Teacher"):
        return False
    
    for _, row in all_teachers.iterrows():
        teacher_id = safe_int(row.get('teacher_id'))
        full_name = f"{safe_str(row.get('first_name'))} {safe_str(row.get('last_name'))}".strip()
        phone_number = safe_str(row.get('phone_number', ''))
        hourly_rate = safe_decimal(row.get('hourly_rate', Decimal('0.0')))
        status = safe_str(row.get('status', 'active'))
        valid_from = safe_datetime_to_date(row.get('valid_from'))
        is_current = safe_int(row.get('is_current', 0))
        updated_at = safe_datetime(row.get('valid_from'))  # используем valid_from как updated_at для таблицы
        
        if valid_from is None:
            valid_from = datetime.now().date()
        
        # Вычисляем valid_to
        if is_current == 1:
            valid_to = future_date
        else:
            # Находим следующую запись с тем же teacher_id для определения valid_to
            same_id_records = all_teachers[all_teachers['teacher_id'] == teacher_id].copy()
            same_id_records = same_id_records.sort_values('valid_from')
            
            current_idx = same_id_records[same_id_records['valid_from'] == row['valid_from']].index
            if not current_idx.empty:
                current_idx = current_idx[0]
                next_records = same_id_records[same_id_records.index > current_idx]
                if not next_records.empty:
                    # valid_to = следующий valid_from - 1 день
                    next_valid_from = next_records.iloc[0]['valid_from']
                    valid_to = next_valid_from.date() - pd.Timedelta(days=1)
                else:
                    valid_to = future_date
            else:
                valid_to = future_date
        
        # Вставляем запись
        if has_user_id_nk:
            insert_query = """
            INSERT INTO Dim_Teacher (
                teacher_sk, teacher_id_nk, user_id_nk, full_name,
                phone_number, hourly_rate, status, 
                valid_from, valid_to, is_current, updated_at
            ) VALUES (
                %(sk)s, %(nk)s, %(user_nk)s, %(full_name)s,
                %(phone_number)s, %(hourly_rate)s, %(status)s, 
                %(valid_from)s, %(valid_to)s, %(is_current)s, %(updated_at)s
            )
            """
            insert_params = {
                'sk': teacher_id,
                'nk': teacher_id,
                'user_nk': safe_int(row.get('user_id')),
                'full_name': full_name,
                'phone_number': phone_number,
                'hourly_rate': hourly_rate,
                'status': status,
                'valid_from': valid_from,
                'valid_to': valid_to,
                'is_current': is_current,
                'updated_at': updated_at
            }
        else:
            insert_query = """
            INSERT INTO Dim_Teacher (
                teacher_sk, teacher_id_nk, full_name,
                phone_number, hourly_rate, status, 
                valid_from, valid_to, is_current, updated_at
            ) VALUES (
                %(sk)s, %(nk)s, %(full_name)s,
                %(phone_number)s, %(hourly_rate)s, %(status)s, 
                %(valid_from)s, %(valid_to)s, %(is_current)s, %(updated_at)s
            )
            """
            insert_params = {
                'sk': teacher_id,
                'nk': teacher_id,
                'full_name': full_name,
                'phone_number': phone_number,
                'hourly_rate': hourly_rate,
                'status': status,
                'valid_from': valid_from,
                'valid_to': valid_to,
                'is_current': is_current,
                'updated_at': updated_at
            }
        
        if not execute_clickhouse_query(insert_query, insert_params):
            return False
        inserted_count += 1
    
    logger.info(f"Dim_Teacher SCD2 load completed: {inserted_count} rows inserted")
    return True

def run_all_loads():
    """Запуск всех загрузок"""
    logger.info("Starting data warehouse load process...")
    
    load_results = {}
    
    # 1. Dim_Subject - полная перезагрузка из последнего файла
    load_results["Dim_Subject"] = load_dim_subject()
    
    # 2. Фактовые таблицы - полная перезагрузка из всех файлов
    load_results["Fact_Homeworks"] = load_fact_homeworks()
    load_results["Fact_Lessons"] = load_fact_lessons()
    load_results["Fact_Sales"] = load_fact_sales()
    
    # 3. SCD2 таблицы - очищаем и загружаем заново все версии
    load_results["Dim_Student"] = load_dim_student()
    load_results["Dim_Teacher"] = load_dim_teacher()
    
    # Логирование результатов
    successful = sum(load_results.values())
    total = len(load_results)
    
    logger.info(f"Load completed: {successful}/{total} successful")
    
    for table, success in load_results.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"  {table}: {status}")
    
    return load_results

def main():
    """Основная функция"""
    try:
        # Проверка подключения к ClickHouse
        clickhouse_client.execute("SELECT 1")
        logger.info("Connected to ClickHouse successfully")
        
        # Запуск загрузок
        results = run_all_loads()
        
        # Проверка результатов
        if all(results.values()):
            logger.info("All tables loaded successfully!")
        else:
            logger.error("Some tables failed to load")
            return 1
            
        return 0
        
    except Exception as e:
        logger.error(f"Critical error in main: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())

from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from my_operators import MyPostgresOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import text

# --- КОНФИГУРАЦИЯ ---
USER_ID = "mirai27" 
TABLE_RAW = f"raw_stats_{USER_ID}"
TABLE_AGG = f"agg_stats_{USER_ID}"

API_URL = "https://b2b.itresume.ru/api/statistics"
API_PARAMS = {
    "client": "Skillfactory",
    "client_key": "M2MGWS"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- ШАБЛОНЫ (JINJA) ---
class WeekTemplates:
    @staticmethod
    def get_start_week():
        return "{{ macros.ds_add(ds, -macros.datetime.strptime(ds, '%Y-%m-%d').weekday()) }}"
    
    @staticmethod
    def get_end_week():
        return "{{ macros.ds_add(ds, 6 - macros.datetime.strptime(ds, '%Y-%m-%d').weekday()) }}"

# --- ФУНКЦИИ ---
def extract_and_load_raw(start_week, end_week, **kwargs):
    # Запрос к API
    params = API_PARAMS.copy()
    params.update({"start": start_week, "end": end_week})
    
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data)
    if df.empty:
        print(f"Данные за период {start_week} - {end_week} отсутствуют")
        return
    
    if 'is_correct' in df.columns:
        df['is_correct'] = df['is_correct'].astype(bool)

    df['upload_period'] = start_week
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        delete_query = text(f"DELETE FROM {TABLE_RAW} WHERE upload_period = :period")
        conn.execute(delete_query, {"period": start_week})
        df.to_sql(TABLE_RAW, conn, if_exists='append', index=False)
    
    print(f"Загружено {len(df)} строк за период {start_week}")


# Функция экспорта принимает ds для имени файла
def export_to_csv(execution_date, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    # Читаем агрегированные данные
    df = pg_hook.get_pandas_df(f"SELECT * FROM {TABLE_AGG}")
    
    if df.empty:
        print("Нет данных для экспорта")
        return

    # Генерируем CSV в памяти
    csv_buffer = df.to_csv(index=False)
    
    # Отправляем в S3 (MinIO)
    s3_hook = S3Hook(aws_conn_id='s3_conn')
    
    # Добавлена проверка/создание бакета для стабильности
    bucket_name = 'my-skillfactory-bucket'
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)

    # Используем execution_date из шаблона {{ ds }}
    file_name = f"reports/agg_results_{USER_ID}_{execution_date}.csv"
    
    s3_hook.load_string(
        string_data=csv_buffer,
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"Файл {file_name} успешно загружен в S3")

# --- DAG ---
with DAG(
    'skillfactory_custom_operator_dag',
    default_args=default_args,
    description='DAG with custom PostgresOperator',
    schedule='@weekly',
    catchup=False
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')

    # 1. Создание таблиц
    create_tables = MyPostgresOperator(
        task_id='create_tables_custom',
        # Мы не указываем conn_id, так как в классе по умолчанию 'postgres_default'
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE_RAW} (
                id SERIAL PRIMARY KEY,
                lti_user_id VARCHAR(255),
                passback_params TEXT,
                is_correct BOOLEAN,
                attempt_type VARCHAR(50),
                created_at TIMESTAMP,
                upload_period DATE
            );
            CREATE TABLE IF NOT EXISTS {TABLE_AGG} (
                period DATE PRIMARY KEY,
                total_attempts INTEGER,
                correct_ratio FLOAT,
                last_updated TIMESTAMP
            );
        """
    )

    # 2. Экстракция
    extract_task = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_and_load_raw,
        op_kwargs={
            'start_week': WeekTemplates.get_start_week(),
            'end_week': WeekTemplates.get_end_week()
        }
    )

    # 3. АГРЕГАЦИЯ ЧЕРЕЗ КАСТОМНЫЙ ОПЕРАТОР
    # Использован MyPostgresOperator с полным SQL запросом
    aggregate_task = MyPostgresOperator(
        task_id='aggregate_data_custom',
        sql=f"""
            INSERT INTO {TABLE_AGG} (period, total_attempts, correct_ratio, last_updated)
            SELECT 
                upload_period, 
                COUNT(*) as total_attempts, 
                AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END) as correct_ratio, 
                NOW()
            FROM {TABLE_RAW}
            GROUP BY upload_period
            ON CONFLICT (period) DO UPDATE SET
                total_attempts = EXCLUDED.total_attempts,
                correct_ratio = EXCLUDED.correct_ratio,
                last_updated = EXCLUDED.last_updated;
        """
    )

    # 4. Экспорт
    export_task = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_to_csv,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    dag_end = EmptyOperator(task_id='dag_end')

    dag_start >> create_tables >> extract_task >> aggregate_task >> export_task >> dag_end
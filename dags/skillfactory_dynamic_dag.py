from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd

# --- КОНФИГУРАЦИЯ БИЗНЕС-КЕЙСА (Python-словарь) ---
USER_ID = "mirai27"

CONFIG = [
    {
        'table_name': f'agg_user_attempts_{USER_ID}',
        'table_ddl': f"""
            CREATE TABLE IF NOT EXISTS agg_user_attempts_{USER_ID} (
                report_date DATE,
                lti_user_id VARCHAR(255),
                attempts_count INTEGER,
                PRIMARY KEY (report_date, lti_user_id)
            );
        """,
        'table_dml': f"""
            SELECT 
                '{{{{ ds }}}}'::DATE as report_date, 
                lti_user_id, 
                COUNT(*) as attempts_count
            FROM raw_stats_{USER_ID}
            WHERE upload_period = '{{{{ macros.ds_add(ds, -macros.datetime.strptime(ds, "%Y-%m-%d").weekday()) }}}}'
            GROUP BY lti_user_id
        """,
        'need_to_export': True, 
    },
    {
        'table_name': f'agg_type_metrics_{USER_ID}',
        'table_ddl': f"""
            CREATE TABLE IF NOT EXISTS agg_type_metrics_{USER_ID} (
                report_date DATE,
                attempt_type VARCHAR(50),
                correct_ratio FLOAT,
                PRIMARY KEY (report_date, attempt_type)
            );
        """,
        'table_dml': f"""
            SELECT 
                '{{{{ ds }}}}'::DATE as report_date, 
                attempt_type, 
                AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END) as correct_ratio
            FROM raw_stats_{USER_ID}
            GROUP BY attempt_type
        """,
        'need_to_export': True, 
    }
]

# --- ОБЩАЯ ФУНКЦИЯ ЭКСПОРТА В S3 ---
def export_table_to_s3(table_name, execution_date, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    query = f"SELECT * FROM {table_name} WHERE report_date = '{execution_date}'"
    df = pg_hook.get_pandas_df(query)
    
    if df.empty:
        print(f"Нет данных для экспорта из таблицы {table_name} за дату {execution_date}")
        return

    csv_buffer = df.to_csv(index=False)
    s3_hook = S3Hook(aws_conn_id='s3_conn')
    bucket_name = 'my-skillfactory-bucket'
    
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)

    file_name = f"reports/dynamic/{table_name}_{execution_date}.csv"
    s3_hook.load_string(
        string_data=csv_buffer, 
        key=file_name, 
        bucket_name=bucket_name, 
        replace=True
    )
    print(f"Таблица {table_name} успешно экспортирована в S3: {file_name}")


# --- НАСТРОЙКИ DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'skillfactory_final_dynamic_dag',
    default_args=default_args,
    description='Финальный проект: Динамическая генерация задач на основе конфига',
    schedule='@weekly',
    catchup=False,
    tags=['final_project', 'dynamic']
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    for table_cfg in CONFIG:
        t_name = table_cfg['table_name']
        
        # 1. Задача автоматического создания таблицы (DDL)
        create_table = SQLExecuteQueryOperator(
            task_id=f'create_table_{t_name}',
            conn_id='postgres_default',
            sql=table_cfg['table_ddl']
        )
        
        # 2. Задача наполнения данными (DML) с обеспечением ИДЕМПОТЕНТНОСТИ.
        idempotent_dml_sql = f"""
            DELETE FROM {t_name} WHERE report_date = '{{{{ ds }}}}';
            INSERT INTO {t_name}
            {table_cfg['table_dml']};
        """
        
        fill_table = SQLExecuteQueryOperator(
            task_id=f'insert_data_{t_name}',
            conn_id='postgres_default',
            sql=idempotent_dml_sql
        )
        
        dag_start >> create_table >> fill_table
        
        # 3. Опциональная задача выгрузки в S3
        if table_cfg['need_to_export']:
            export_s3 = PythonOperator(
                task_id=f'export_to_s3_{t_name}',
                python_callable=export_table_to_s3,
                op_kwargs={
                    'table_name': t_name,
                    'execution_date': '{{ ds }}'
                }
            )
            fill_table >> export_s3 >> dag_end
        else:
            fill_table >> dag_end
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook\


class MyPostgresOperator(BaseOperator):
    """
    Кастомный оператор для выполнения SQL запросов в Postgres.
    Не возвращает данные (предназначен для INSERT, UPDATE, DELETE, CREATE).
    """
    # Указываем, какие поля поддерживают Jinja-шаблоны
    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#74a6d4" # Цвет кубика в интерфейсе Airflow

    def __init__(self, sql, postgres_conn_id='postgres_default', **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        self.log.info(f"Executing SQL: {self.sql}")
        # Создаем хук внутри метода execute
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # Выполняем запрос без возврата данных
        hook.run(self.sql)
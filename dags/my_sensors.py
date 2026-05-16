from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

class MyMultiSqlSensor(BaseSensorOperator):
    """
    Кастомный сенсор, выполняющий несколько SQL-запросов.
    Каждый запрос должен вернуть хотя бы одну запись (или значение отличное от 0/False).
    Поддерживает Jinja-шаблонизацию (например, {{ ds }}).
    """
    # Важно: объявляем sql_queries как template_field, чтобы внутри запросов работал {{ ds }}
    template_fields = ('sql_queries',)

    def __init__(self, sql_queries: list[str], conn_id: str = 'postgres_default', **kwargs):
        kwargs['mode'] = kwargs.get('mode', 'reschedule') # Всегда reschedule
        super().__init__(**kwargs)
        self.sql_queries = sql_queries
        self.conn_id = conn_id

    def poke(self, context: Context) -> bool:
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info("Запуск проверки данных...")
        
        for sql in self.sql_queries:
            self.log.info(f"Выполнение проверочного запроса: {sql}")
            try:
                record = hook.get_first(sql)
                
                # Если запрос вообще ничего не вернул (0 строк)
                if not record:
                    self.log.info("Запрос вернул 0 строк. Данные еще не готовы. Reschedule.")
                    return False
                
                # Если это был запрос вида SELECT COUNT(1) и он вернул [0]
                first_cell = record[0]
                if str(first_cell) in ('0', 'False', 'None') or not first_cell:
                    self.log.info(f"Запрос вернул пустое значение ({first_cell}). Reschedule.")
                    return False
                
                self.log.info("Проверка данного запроса успешно пройдена.")
                
            except Exception as e:
                self.log.error(f"Ошибка при выполнении запроса: {str(e)}")
                return False
                
        self.log.info("Все проверки пройдены! Выполнение сенсора завершено успешно.")
        return True
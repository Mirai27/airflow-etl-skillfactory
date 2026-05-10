FROM apache/airflow:3.2.1
ADD requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
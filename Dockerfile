FROM apache/airflow:2.5.3
COPY requirements.txt /
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
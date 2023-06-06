FROM apache/airflow:2.6.1-python3.9
COPY requirements.txt /
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
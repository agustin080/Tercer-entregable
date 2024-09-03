FROM python:3.10.5-slim-buster

COPY . usr/src/app
WORKDIR /usr/src/app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install python-dotenv
RUN pip install apache-airflow==2.10.0

COPY dags/ /usr/local/airflow/dags/

EXPOSE 8080

CMD ["airflow", "webserver", "--port", "8080"]
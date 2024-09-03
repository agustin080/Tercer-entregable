from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from functions import fetch_weather_data, process_weather_data, load_data_to_redshift
import os
from datetime import datetime, timedelta
import random

# Configuración de DAG
default_args={
    'owner': 'Agustin',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='Un DAG para obtener y cargar datos del clima',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024,9,3),
    catchup=False,
)

# Función para ejecutar el proceso ETL
def run_etl():
    cities = [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
        'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
        'Austin', 'Jacksonville', 'San Francisco', 'Columbus', 'Indianapolis',
        'Fort Worth', 'Charlotte', 'Seattle', 'Denver', 'El Paso',
        'Detroit', 'Boston', 'Memphis', 'Nashville', 'Baltimore',
        'Oklahoma City', 'Las Vegas', 'Louisville', 'Milwaukee', 'Albuquerque',
        'Tucson', 'Fresno', 'Sacramento', 'Kansas City', 'Mesa',
        'Virginia Beach', 'Atlanta', 'Colorado Springs', 'Omaha', 'Raleigh',
        'Miami', 'Cleveland', 'Tulsa', 'Oakland', 'Minneapolis',
        'Wichita', 'Arlington', 'Bakersfield', 'Tampa', 'Aurora',
        'Honolulu', 'Anaheim', 'Toledo', 'Santa Ana', 'St. Louis', 'Riverside',
        'Corpus Christi', 'Lexington', 'Stockton', 'Henderson', 'Saint Paul',
        'Cincinnati', 'Pittsburgh', 'Greensboro', 'Anchorage', 'Plano',
        'Newark', 'Lincoln', 'Orlando', 'Chula Vista',
        'Jersey City', 'Buffalo', 'Durham', 'Madison', 'Lubbock',
        'Montreal', 'Toronto', 'Vancouver', 'Calgary', 'Edmonton',
        'Ottawa', 'Winnipeg', 'Halifax', 'Quebec City', 'Victoria',
        'London', 'Manchester', 'Birmingham', 'Glasgow', 'Liverpool',
        'Edinburgh', 'Belfast', 'Cardiff', 'Dublin', 'Limerick',
        'Paris', 'Marseille', 'Lyon', 'Toulouse', 'Nice',
        'Berlin', 'Hamburg', 'Munich', 'Cologne', 'Frankfurt',
        'Vienna', 'Zurich', 'Geneva', 'Brussels', 'Amsterdam',
        'Rome', 'Milan', 'Naples', 'Turin', 'Florence'
    ]

    # Seleccionar 10 ciudades al azar
    random_cities = random.sample(cities, 10)

    # Obtener datos del clima
    weather_data_list = fetch_weather_data(random_cities)

    # Procesar los datos del clima
    data = process_weather_data(weather_data_list)

    # Convertir a DataFrame
    df = pd.DataFrame(data)

    # Cargar datos en Redshift
    load_data_to_redshift(df)

# Crear una tarea PythonOperator para ejecutar el ETL
run_etl_task = PythonOperator(
    task_id='run_etl_task',
    python_callable=run_etl,
    dag=dag,
)

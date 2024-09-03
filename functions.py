import requests
from datetime import datetime
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import random

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de la API
api_key = os.getenv('OPENWEATHER_API_KEY')
url = 'https://api.openweathermap.org/data/2.5/weather'

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
        'Honolulu', 'Anaheim', 'Santa Ana', 'St. Louis', 'Riverside',
        'Corpus Christi', 'Lexington', 'Stockton', 'Henderson', 'Saint Paul',
        'Cincinnati', 'Pittsburgh', 'Greensboro', 'Anchorage', 'Plano',
        'Newark', 'Lincoln', 'Toledo', 'Orlando', 'Chula Vista',
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

def fetch_random_city_weather_data():
    """Obtiene datos del clima de OpenWeatherMap para una ciudad aleatoria."""
    city = random.choice(cities)
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'  # Unidades en grados Celsius
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error al obtener datos del clima para {city}: {response.status_code}")
        return None

def process_weather_data(weather_data_list):
    """Procesa los datos del clima y devuelve una lista de diccionarios."""
    data = []
    for weather_data in weather_data_list:
        if weather_data:
            entry = {
                'city': weather_data['name'],
                'temperature': weather_data['main']['temp'],
                'humidity': weather_data['main']['humidity'],
                'weather_description': weather_data['weather'][0]['description'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            data.append(entry)
    return data

def truncate_string(value, length):
    """Trunca una cadena a un tamaño específico."""
    return value[:length] if value and len(value) > length else value

def load_data_to_redshift(df):
    """Carga o actualiza los datos del DataFrame en la base de datos Redshift."""
    redshift_credentials = {
        'user': os.getenv('REDSHIFT_USER'),
        'host': os.getenv('REDSHIFT_HOST'),
        'pass': os.getenv('REDSHIFT_PASS'),
        'db': os.getenv('REDSHIFT_DB'),
        'port': os.getenv('REDSHIFT_PORT')
    }

    try:
        conn = psycopg2.connect(
            dbname=redshift_credentials['db'],
            user=redshift_credentials['user'],
            password=redshift_credentials['pass'],
            host=redshift_credentials['host'],
            port=redshift_credentials['port']
        )
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            city VARCHAR(255),
            temperature FLOAT,
            humidity INTEGER,
            weather_description VARCHAR(255),
            timestamp TIMESTAMP,
            PRIMARY KEY (city, timestamp)
        )
        ''')

        for index, row in df.iterrows():
            if row.isnull().any():
                print(f"Fila {index} omitida debido a valores nulos: {row.to_dict()}")
                continue

            city = truncate_string(row['city'], 255)
            weather_description = truncate_string(row['weather_description'], 255)

            cursor.execute('''
            DELETE FROM weather WHERE city = %s AND timestamp = %s
            ''', (city, row['timestamp']))

            cursor.execute('''
            INSERT INTO weather (city, temperature, humidity, weather_description, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ''', (city, row['temperature'], row['humidity'], weather_description, row['timestamp']))

        conn.commit()

    except Exception as e:
        print(f"Error al cargar los datos en Redshift: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
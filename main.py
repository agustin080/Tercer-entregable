from functions import fetch_random_city_weather_data, process_weather_data, load_data_to_redshift
import pandas as pd

def main():
    """Función principal para ejecutar el proceso ETL."""
    # Obtener datos del clima para una ciudad aleatoria
    weather_data = fetch_random_city_weather_data()
    
    if weather_data:
        # Procesar los datos del clima
        data = process_weather_data([weather_data])

        # Convertir a DataFrame
        df = pd.DataFrame(data)

        # Cargar los datos en Redshift
        load_data_to_redshift(df)

if __name__ == "__main__":
    main()
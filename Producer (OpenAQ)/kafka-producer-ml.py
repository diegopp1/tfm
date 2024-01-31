import time
import requests
from confluent_kafka import Producer
from datetime import datetime, timedelta

# Configuración del productor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(conf)

# Países y parámetros de interés
countries = ['CA', 'CL', 'DE', 'FR', 'IT', 'US', 'ES', 'PT', 'CN', 'JP', 'BR', 'AU', 'ZA']
parameters = ['pm25', 'pm10', 'um100']

# API Key de OpenAQ (reemplaza 'your-openaq-api-key' con tu clave real)
openaq_api_key = '83fcfc1c531d71a7290846eb31fd75b91a3f1cd85653f2fef21f5140e2371746'

# Límites de uso de la API de OpenAQ
requests_limit = 300
window_duration = 300  # segundos (5 minutos)

# Función para obtener datos de OpenAQ y producir mensajes en Kafka
def fetch_and_produce_openaq_data(country, parameter, start_date, end_date, limit=10):
    base_url = 'https://api.openaq.org/v2/measurements'
    date_format = '%Y-%m-%dT%H:%M:%S'

    current_date = start_date
    print(f"Obteniendo datos de OpenAQ para {country} y {parameter}")
    print (f"Fecha de inicio: {start_date.strftime(date_format)}")
    while current_date <= end_date:
        formatted_date = current_date.strftime(date_format)
        print(f"Fecha actual: {formatted_date}")
        params = {
            'date_from': formatted_date,
            'date_to': (current_date + timedelta(days=1)).strftime(date_format),
            'limit': limit,
            'parameter': parameter,
            'country': country,
        }

        try:
            response = requests.get(base_url, params=params, headers={"X-API-Key": openaq_api_key})
            print(response.status_code)
            if response.status_code == 200:
                openaq_data = response.json().get('results', [])
                for entry in openaq_data:
                    message = f"{country}-{parameter}-{formatted_date}: {entry}"
                    producer.produce('data-2019', key=None, value=message.encode('utf-8'))
                    print(f"Enviado a Kafka: {message}")
            else:
                print(f"Error al obtener datos de OpenAQ. Código de estado: {response.status_code}")
        except Exception as e:
            print(f"Error en la solicitud a la API de OpenAQ: {e}")

        current_date += timedelta(days=1)
        time.sleep(10)  # Espera 10 segundos entre cada solicitud para cumplir con los límites de uso

if __name__ == '__main__':
    # Fecha de inicio y fin (por ejemplo, para el año 2019)
    start_date = datetime(2019, 1, 1)
    end_date = datetime(2019, 12, 31)

    # Bucle para cambiar país y parámetro cada vez
    for country in countries:
        for parameter in parameters:
            fetch_and_produce_openaq_data(country, parameter, start_date, end_date)
            time.sleep(10)  # Espera 10 segundos antes de cambiar al siguiente país y parámetro

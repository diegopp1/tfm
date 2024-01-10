import requests
from confluent_kafka import Producer
import json
import time

# Configuración del productor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
}

# Crear un productor
producer = Producer(conf)

# API Key de OpenAQ (reemplaza 'your-openaq-api-key' con tu clave real)
openaq_api_key = '83fcfc1c531d71a7290846eb31fd75b91a3f1cd85653f2fef21f5140e2371746'

# Nombre del tema de Kafka (reemplaza 'my_topic' con el nombre real del tema)
kafka_topic = 'my_topic'

# URL base de la API de OpenAQ para obtener datos de calidad del aire por país
openaq_data_url = "https://api.openaq.org/v2/measurements"


def fetch_air_quality_data(country_code):
    # Realizar la solicitud a la API de OpenAQ con la clave API y el código de país específico
    params = {
        'country': country_code,
        'limit': 1,  # Puedes ajustar el límite según tus necesidades
        'sort': 'desc',  # Puedes ajustar el orden según tus necesidades
    }
    response = requests.get(openaq_data_url, headers={"X-API-Key": openaq_api_key}, params=params)

    if response.status_code == 200:
        return response.json()['results']
    else:
        raise Exception(
            f"Error al obtener datos de calidad del aire de OpenAQ. Código de estado: {response.status_code}")


def delivery_report(err, msg):
    if err is not None:
        print('Error al entregar el mensaje: {}'.format(err))
    else:
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))


while True:
    try:
        # Obtener la lista de países de OpenAQ
        countries = fetch_countries()

        # Iterar sobre la lista de países
        for country in countries:
            country_code = country['code']

            # Obtener datos de calidad del aire para el país específico
            air_quality_data = fetch_air_quality_data(country_code)

            # Enviar datos al tema de Kafka
            producer.produce(kafka_topic, key=None, value=json.dumps(air_quality_data).encode('utf-8'),
                             callback=delivery_report)

            # Esperar un breve período de tiempo antes de obtener nuevos datos
            time.sleep(1)  # Espera 1 segundo entre cada país (puedes ajustar este valor)

    except Exception as e:
        print(f"Error al obtener/enviar datos: {e}")

# Esperar a que todos los mensajes se entreguen antes de cerrar el productor
producer.flush()


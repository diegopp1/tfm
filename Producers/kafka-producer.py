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
openaq_api_key = 'your-openaq-api-key'

# URL de la API de OpenAQ
openaq_url = "https://api.openaq.org/v2/locations/"

# Lista de regiones geográficas
geographic_regions = ['europa', 'asia', 'africa', 'oceania']

def fetch_openaq_data(region):
    # Realizar la solicitud a la API de OpenAQ con la clave API y la región específica
    response = requests.get(openaq_url + region, headers={"X-API-Key": openaq_api_key})
    return response.json()

def delivery_report(err, msg):
    if err is not None:
        print('Error al entregar el mensaje: {}'.format(err))
    else:
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))

while True:
    try:
        # Iterar sobre las regiones y obtener datos de OpenAQ para cada región
        for region in geographic_regions:
            openaq_data = fetch_openaq_data(region)

            # Enviar datos al tema de Kafka específico para la región
            kafka_topic = f'openaq_{region}'
            producer.produce(kafka_topic, key=None, value=json.dumps(openaq_data).encode('utf-8'), callback=delivery_report)

        # Esperar un breve período de tiempo antes de obtener nuevos datos
        time.sleep(60)  # Espera 60 segundos (puedes ajustar este valor)

    except Exception as e:
        print(f"Error al obtener/enviar datos: {e}")

# Esperar a que todos los mensajes se entreguen antes de cerrar el productor
producer.flush()

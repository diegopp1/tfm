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

# URL base de la API de OpenAQ
openaq_base_url = "https://api.openaq.org/v2/locations/"

# Nombre del tema de Kafka (reemplaza 'my_topic' con el nombre real del tema)
kafka_topic = 'my_topic'


def fetch_openaq_data(location_id):
    # Realizar la solicitud a la API de OpenAQ con la clave API y el ID de la ubicación específica
    response = requests.get(f"{openaq_base_url}{location_id}", headers={"X-API-Key": openaq_api_key})

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error al obtener datos de OpenAQ. Código de estado: {response.status_code}")


def delivery_report(err, msg):
    if err is not None:
        print('Error al entregar el mensaje: {}'.format(err))
    else:
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))


while True:
    try:
        # Obtener datos de OpenAQ para la ubicación específica
        # En este ejemplo, la ubicación se deja como un parámetro configurable (puede ser seleccionado desde el front end)
        location_id = input("Introduce el ID de la ubicación de OpenAQ (por ejemplo, 2178): ")
        openaq_data = fetch_openaq_data(location_id)

        # Enviar datos al tema de Kafka
        producer.produce(kafka_topic, key=None, value=json.dumps(openaq_data).encode('utf-8'), callback=delivery_report)

        # Esperar un breve período de tiempo antes de obtener nuevos datos
        time.sleep(60)  # Espera 60 segundos (puedes ajustar este valor)

    except Exception as e:
        print(f"Error al obtener/enviar datos: {e}")

# Esperar a que todos los mensajes se entreguen antes de cerrar el productor
producer.flush()

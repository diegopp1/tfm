import json
import time
import requests
from confluent_kafka import Producer
import logging

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('producer')

# Configuración del productor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
}

# Crear un productor
producer = Producer(conf)

# Nombre del tema de Kafka
kafka_topic = 'locations'

# API Key de OpenAQ (reemplaza 'your-openaq-api-key' con tu clave real)
openaq_api_key = '83fcfc1c531d71a7290846eb31fd75b91a3f1cd85653f2fef21f5140e2371746'

# URL base de la API de OpenAQ para obtener datos de calidad del aire por país
openaq_data_url = "https://api.openaq.org/v2/locations?limit=10&page=1&offset=0&sort=desc&parameter=&radius=1000&country={}&order_by=lastUpdated&dump_raw=false"

def fetch_openaq_data(country):
    try:
        response = requests.get(openaq_data_url.format(country), headers={"X-API-Key": openaq_api_key})

        if response.status_code == 200:
            return response.json()['results']
        else:
            logger.error(f"Error al obtener datos de calidad del aire de OpenAQ. Código de estado: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error en la solicitud a la API de OpenAQ: {e}")
        return None

def delivery_report(err, msg):
    if err is not None:
        logger.error('Error al entregar el mensaje: {}'.format(err))
    else:
        logger.info('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))
        # Imprimir el contenido del mensaje (clave y valor)
        logger.info('Contenido del mensaje: Key: {}, Value: {}'.format(msg.key(), msg.value()))

# Función para obtener el país seleccionado (puedes personalizar esto según tu aplicación)
def get_selected_country():
    # Por ahora, se devuelve 'US' como valor predeterminado.
    return selected_country or 'US'

# Bucle principal para enviar datos de OpenAQ al tema de Kafka
while True:
    try:
        selected_country = get_selected_country()

        openaq_data = fetch_openaq_data(selected_country)
        print(openaq_data)
        if openaq_data is not None:
            # Enviar datos al tema de Kafka
            for data_entry in openaq_data:
                producer.produce(
                    kafka_topic,
                    key=None,
                    value=json.dumps(data_entry).encode('utf-8'),
                    callback=delivery_report
                )
                print(f"Enviando datos de calidad del aire a Kafka para {selected_country}")
        else:
            logger.warning("No se pudieron obtener datos de calidad del aire. Reintentando en 60 segundos.")

        # Esperar diez minutos antes de obtener nuevos datos
        time.sleep(10)
    except Exception as e:
        logger.error(f"Error al obtener/enviar datos: {e}")

# Cerrar el productor al salir del bucle principal
producer.flush()


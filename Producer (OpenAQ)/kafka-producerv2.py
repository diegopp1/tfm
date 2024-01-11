import json
import time
from confluent_kafka import Producer

# Configuración del productor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
}

# Crear un productor
producer = Producer(conf)

# Nombre del tema de Kafka (reemplaza 'my_topic' con el nombre real del tema)
kafka_topic = 'json-topic'


def fetch_data():
    # Esta función debería contener tu lógica para obtener datos simulados o reales
    # Reemplaza esto con tu propia lógica
    simulated_data = {
        'location': 'Simulated Location',
        'value': 42.0,
        'unit': 'Simulated Unit',
    }
    return simulated_data


def delivery_report(err, msg):
    if err is not None:
        print('Error al entregar el mensaje: {}'.format(err))
    else:
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))


# Bucle principal para enviar datos simulados (ajusta según tu caso)
while True:
    try:
        # Obtener datos simulados (reemplaza esto con tu lógica real)
        air_quality_data = fetch_data()

        # Enviar datos al tema de Kafka
        producer.produce(
            kafka_topic,
            key=None,
            value=json.dumps(air_quality_data).encode('utf-8'),
            callback=delivery_report
        )

        # Esperar un segundo antes de obtener nuevos datos
        time.sleep(1)  # Espera 1 segundo entre cada envío

    except Exception as e:
        print(f"Error al obtener/enviar datos: {e}")

# Cerrar el productor al salir del bucle principal
producer.flush()

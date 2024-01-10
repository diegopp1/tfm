from confluent_kafka import Consumer, KafkaError
import requests
import json

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
topic = 'json-topic'  # Cambia esto según el nombre del tema que creaste en el clúster Kafka
# Crear un consumidor
consumer = Consumer(conf)

# Suscribirse a un tema (reemplaza 'my_topic' con el nombre real del tema)
consumer.subscribe([topic])

while True:
    # Esperar mensajes
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # No se encontraron nuevos mensajes
            continue
        else:
            print(msg.error())
            break

    # Decodificar y procesar el mensaje JSON
    try:
        data = json.loads(msg.value().decode('utf-8'))

        # Aquí puedes realizar operaciones con los datos, como enviarlos a una base de datos o procesarlos de alguna otra manera
        print("Datos recibidos:", data)

    except Exception as e:
        print(f"Error al procesar mensaje: {e}")

# Cerrar el consumidor
consumer.close()

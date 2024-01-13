from confluent_kafka import Consumer, KafkaError
import json
import logging

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
topic = 'datos'  # Cambia esto según el nombre del tema que creaste en el clúster Kafka

# Crear un consumidor
consumer = Consumer(conf)

# Suscribirse a un tema
consumer.subscribe([topic])

try:
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
                logger.error(msg.error())
                break

        # Decodificar y procesar el mensaje JSON
        try:
            data = json.loads(msg.value().decode('utf-8'))
            logger.info("Datos recibidos: {}".format(data))
        except Exception as e:
            logger.error(f"Error al procesar mensaje: {e}")

except KeyboardInterrupt:
    pass  # Manejar la interrupción del teclado (Ctrl+C)

finally:
    # Cerrar el consumidor
    consumer.close()


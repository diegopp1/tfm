import logging
from confluent_kafka import Consumer, KafkaException

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'openaq_consumer_group',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(conf)

# Suscribirse al tema 'data-2019'
topic_name = 'data-2019'
consumer.subscribe([topic_name])

# Bucle principal para consumir mensajes
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException.PARTITION_EOF:
                # Fin de la partición, no es un error
                continue
            else:
                logger.error(msg.error())
                break

        # Registro del mensaje recibido
        logger.info('Mensaje recibido: {}'.format(msg.value().decode('utf-8')))

except KeyboardInterrupt:
    pass

finally:
    # Cerrar el consumidor
    consumer.close()

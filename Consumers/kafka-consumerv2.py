from confluent_kafka import Consumer, KafkaError
import json


# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
topic = 'topic-1'  # Cambia esto según el nombre del tema que creaste en el clúster Kafka

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
                print(msg.error())
                break

        # Decodificar y procesar el mensaje JSON
        try:
            data = json.loads(msg.value().decode('utf-8'))


        except Exception as e:
            print(f"Error al procesar mensaje: {e}")

except KeyboardInterrupt:
    pass  # Manejar la interrupción del teclado (Ctrl+C)

finally:
    # Cerrar el consumidor
    consumer.close()

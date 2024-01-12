from fastapi import FastAPI, WebSocket, Depends
from confluent_kafka import Consumer, KafkaException, KafkaError
import json

app = FastAPI()

# Configuración del consumidor de Kafka
kafka_topic = 'topic-2'  # Cambia esto según el nombre del tema que creaste en el clúster Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe([kafka_topic])

# WebSocket endpoint para la transmisión de datos
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    # Ciclo para recibir datos del consumidor de Kafka y enviarlos al frontend
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # No se encontraron nuevos mensajes
                        continue
                    else:
                        raise KafkaException(msg.error())

                # Decodificar y procesar el mensaje JSON
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    await websocket.send_json(data)
                except Exception as e:
                    print(f"Error al procesar mensaje: {e}")
    except KafkaException as ke:
        print(f"Error en el consumidor de Kafka: {ke}")
    finally:
        consumer.close()


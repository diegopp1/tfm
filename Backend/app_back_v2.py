from fastapi import FastAPI, WebSocket, Depends
from kafka import KafkaConsumer
import json

app = FastAPI()

# Configuración del consumidor de Kafka
kafka_topic = 'topic-2'  # Cambia esto según el nombre del tema que creaste en el clúster Kafka
consumer = KafkaConsumer(kafka_topic, bootstrap_servers='localhost:9092', group_id='my_consumer_group', auto_offset_reset='earliest')

# WebSocket endpoint para la transmisión de datos
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    # Ciclo para recibir datos del consumidor de Kafka y enviarlos al frontend
    while True:
        for message in consumer:
            try:
                data = json.loads(message.value.decode('utf-8'))
                await websocket.send_json(data)
            except Exception as e:
                print(f"Error al procesar mensaje: {e}")

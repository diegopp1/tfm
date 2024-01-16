from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from confluent_kafka import Producer, Consumer, KafkaError
import json
import logging
import threading
import subprocess
import os

app = Flask(__name__)
socketio = SocketIO(app, threaded=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
kafka_topic = 'datos'

producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Crear el consumidor de Kafka
consumer = Consumer(kafka_conf)
consumer.subscribe([kafka_topic])

devices_by_location = {}
producer_running = False

def consume_message():
    msg = consumer.poll(1.0)
    if msg is not None and not msg.error():
        try:
            data = json.loads(msg.value().decode('utf-8'))
            logger.info("Datos recibidos: {}".format(data))
            return data
        except Exception as e:
            logger.error(f"Error al procesar mensaje: {e}")
    return None

def background_thread():
    while True:
        data = consume_message()
        if data is not None:
            socketio.emit('air_quality_data', data)
            handle_devices(data)
            socketio.sleep(1)

def handle_devices(data):
    location_id = data.get('locationId')
    country = data.get('country')

    if location_id not in devices_by_location:
        devices_by_location[location_id] = {
            'locationId': location_id,
            'location': data.get('location'),
            'country': country,
            'devices': []
        }

    devices = devices_by_location[location_id]['devices']
    if data not in devices:
        devices.append(data)

    socketio.emit('device_info', devices_by_location[location_id])

@app.route('/')
def index():
    global producer_running
    if not producer_running:
        start_producer()
        producer_running = True

    return render_template('index2.html')

def start_producer():
    if not is_producer_running():
        script_path = 'C:\\Users\\Usuario\\PycharmProjects\\pythonProject2\\Producer (OpenAQ)\\kafka-producerv3.py'
        subprocess.Popen(['python', script_path])
        print("Productor de Kafka iniciado.")
    else:
        print("El productor ya está en ejecución.")

def is_producer_running():
    # Lógica para verificar si el script de Producer ya está en ejecución
    return False  # Devuelve True si el productor está en ejecución, False de lo contrario

@socketio.on('connect')
def handle_connect():
    print('Cliente conectado')
    emit('status', {'data': 'Conexión establecida'})

if __name__ == '__main__':
    socketio.start_background_task(target=background_thread)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False) # Esta sentencia es necesaria para que el servidor Flask
    # y el SocketIO funcionen correctamente


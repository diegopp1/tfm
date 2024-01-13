from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaError
import json
import logging

app = Flask(__name__)  # Inicializar la aplicación Flask
socketio = SocketIO(app, threaded=True)  # Inicializar SocketIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
kafka_topic = 'datos'

consumer = Consumer(kafka_conf)  # Crear un consumidor
consumer.subscribe([kafka_topic])  # Suscribirse a un tema

# Diccionario para almacenar dispositivos por locationId
devices_by_location = {}

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

    # Filtrar duplicados por locationId
    if location_id not in devices_by_location:
        devices_by_location[location_id] = {
            'locationId': location_id,
            'location': data.get('location'),
            'country': country,
            'devices': []
        }

    # Agregar dispositivo a la lista si no está duplicado
    devices = devices_by_location[location_id]['devices']
    if data not in devices:
        devices.append(data)

    # Enviar información al frontend para la gestión de dispositivos
    socketio.emit('device_info', devices_by_location[location_id])

@app.route('/')
def index():
    return render_template('index2.html')  # Renderizar la plantilla index.html

@app.route('/devices')
def devices():
    return render_template('devices.html', devices=devices_by_location.values())

@socketio.on('connect')
def handle_connect():
    print('Cliente conectado')
    emit('status', {'data': 'Conexión establecida'})

if __name__ == '__main__':
    socketio.start_background_task(target=background_thread)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)

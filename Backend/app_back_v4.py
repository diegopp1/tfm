from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer
import json
import logging
import subprocess

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
            socketio.sleep(1)

@app.route('/')
def index():
    global producer_running
    if not producer_running:
        start_producer()
        producer_running = True

    return render_template('index2.html')

@app.route('/management')
def management():
    data = consume_message()
    if data is not None:
        handle_devices(data)
        updated_devices = devices_by_location.get(data.get('location'), {}).get('devices', [])
        return render_template('devices.html', devices=updated_devices) # Actualizar la página de dispositivos
    else:
        return render_template('devices.html', devices=[], error='No se han recibido datos de calidad del aire.')

def handle_devices(data):
    location = data.get('location')
    country = data.get('country')

    if location not in devices_by_location:
        devices_by_location[location] = {
            'location': location,
            'country': country,
            'devices': []
        }

    devices = devices_by_location[location]['devices']
    if data not in devices:
        devices.append(data)

    socketio.emit('device_info', devices_by_location[location])

@socketio.on('connect')
def handle_connect():
    print('Cliente conectado')
    emit('status', {'data': 'Conexión establecida'})

def start_producer():
    if not is_producer_running():
        script_path = 'C:\\Users\\Usuario\\PycharmProjects\\pythonProject2\\Producer (OpenAQ)\\kafka-producerv3.py'
        subprocess.Popen(['python', script_path])
        print("Productor de Kafka iniciado.")
    else:
        print("El productor ya está en ejecución.")

def is_producer_running():
    # Lógica para verificar si el productor ya está en ejecución
    # Puedes ajustar esta lógica según tus necesidades
    # Puedes usar bibliotecas como psutil o consultar el sistema operativo
    return False  # Devuelve True si el productor está en ejecución, False de lo contrario

if __name__ == '__main__':
    socketio.start_background_task(target=background_thread)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)

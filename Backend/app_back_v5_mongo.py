from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaException
from pymongo.mongo_client import MongoClient
import json
import logging
import subprocess
from decouple import config

app = Flask(__name__)
socketio = SocketIO(app, threaded=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

# Configuración de Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
kafka_topic = 'datos'

# Configuración de MongoDB
mongo_password = config('MONGO_PASSWORD', default='')
mongo_uri = f"mongodb+srv://diegopp1:{mongo_password}@cluster0.omhmfeu.mongodb.net/?retryWrites=true&w=majority"
mongo_client = MongoClient(mongo_uri)

try:
    # Verificación de la conexión a MongoDB al iniciar la aplicación
    mongo_client.admin.command('ping')
    logger.info("Conexión a MongoDB establecida exitosamente!")
except Exception as e:
    logger.error("Error conectando a MongoDB:", e)
    raise SystemExit("No se pudo conectar a MongoDB. Saliendo...")

mongo_db = mongo_client['iot_data']
mongo_collection = mongo_db['air_quality']

# Crear el consumidor de Kafka
consumer = Consumer(kafka_conf)

try:
    consumer.subscribe([kafka_topic])
except KafkaException as e:
    logger.error("Error al suscribirse al tema de Kafka:", e)
    raise SystemExit("No se pudo suscribir al tema de Kafka. Saliendo...")

devices_by_location = {}
producer_running = False

def consume_message():
    try:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            data = json.loads(msg.value().decode('utf-8'))
            logger.info("Datos recibidos: {}".format(data))
            # Guardar datos en MongoDB
            mongo_collection.insert_one(data)
            return data
    except Exception as e:
        logger.error(f"Error al procesar mensaje de Kafka: {e}")
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

@app.route('/data')
def data():
    # Obtener todos los datos almacenados en MongoDB
    stored_data = list(mongo_collection.find())
    return render_template('data.html', data=stored_data)

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

@socketio.on('disconnect')
def handle_disconnect():
    print('Cliente desconectado')

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



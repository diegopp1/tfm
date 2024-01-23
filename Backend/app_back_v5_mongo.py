from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaException
from pymongo.mongo_client import MongoClient
from bson import ObjectId
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
            # Convertir ObjectId a cadena antes de emitir
            data['_id'] = str(data['_id'])
            socketio.emit('air_quality_data', data)
            handle_devices(data)
            socketio.sleep(1)

@app.route('/')
def index():
    global producer_running
    if not producer_running:
        start_producer()
        producer_running = True

    return render_template('index2.html')

devices = []  # Lista global de dispositivos
device_id_counter = 1  # Inicializar un contador para generar IDs únicos

@app.route('/management', methods=['GET', 'POST'])
def management():
    global device_id_counter
    global devices
    if request.method == 'POST':
        # Agregar dispositivo
        new_device = {
            'id': device_id_counter,
            'location': request.form['location'],
            'country': request.form['country']
        }
        devices.append(new_device)
        device_id_counter += 1
        socketio.emit('device_info', {'devices': devices}, broadcast=True)

    elif request.method == 'GET' and 'delete_device' in request.args:
        # Eliminar dispositivo
        device_id = int(request.args['delete_device'])
        devices = [device for device in devices if device['id'] != device_id]
        socketio.emit('device_info', {'devices': devices}, broadcast=True)

    return render_template('devices.html', devices=devices)
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



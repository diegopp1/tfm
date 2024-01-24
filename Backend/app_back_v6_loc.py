from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaException, Producer
from pymongo.mongo_client import MongoClient
import json
import logging
import subprocess
from decouple import config

app = Flask(__name__)
socketio = SocketIO(app, threaded=True)

producer_running = False
sec_producer_running = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

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
mongo_air_quality_collection = mongo_db['air_quality']
mongo_locations_collection = mongo_db['locations']

# Crear el consumidor de Kafka
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
})

try:
    consumer.subscribe(['locations', 'datos'])
except KafkaException as e:
    logger.error("Error al suscribirse a los temas de Kafka:", e)
    raise SystemExit("No se pudo suscribir a los temas de Kafka. Saliendo...")

devices_by_location = {}


def consume_message():
    try:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            data = json.loads(msg.value().decode('utf-8'))
            logger.info("Datos recibidos: {}".format(data))
            return data
    except Exception as e:
        logger.error(f"Error al procesar mensaje de Kafka: {e}")
    return None


def background_thread():
    while True:
        data = consume_message()
        if data is not None:
            # Verificar si '_id' está presente en el diccionario
            if '_id' in data:
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


@app.route('/locations')
def locations():
    global sec_producer_running
    selected_country = request.args.get('country')  # Cambiado de request.form a request.args
    if not sec_producer_running:
        start_second_producer(selected_country)
        sec_producer_running = True
    return render_template('locations.html')


@app.route('/generate', methods=['POST'])
def generate_data():
    selected_country = request.form.get('country')  # Obtener el país seleccionado por el usuario
    start_second_producer(selected_country)  # Iniciar el segundo productor de Kafka
    return 'Generating data...'


@app.route('/data')
def data():
    # Obtener todos los datos almacenados en MongoDB
    stored_data = list(mongo_locations_collection.find())
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

    devices = devices_by_location[location]['devices']  # Obtener los dispositivos de la ubicación
    if data not in devices:  # Evitar duplicados
        devices.append(data)
        # Guardar datos del dispositivo en la colección locations
        mongo_locations_collection.insert_one(data)

    socketio.emit('devices', devices_by_location[location])  # Emitir los dispositivos de la ubicación


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


def start_second_producer(country):
    selected_country = request.form.get('country')
    if not is_second_producer_running() and selected_country is not None:
        script_path = 'C:\\Users\\Usuario\\PycharmProjects\\pythonProject2\\Producer (OpenAQ)\\kafka-producer-loc.py'
        subprocess.Popen(['python', script_path, country])
        print("Segundo productor de Kafka iniciado.")
    else:
        print("El segundo productor ya está en ejecución.")


def is_producer_running():
    # Lógica para verificar si el productor ya está en ejecución
    # Puedes ajustar esta lógica según tus necesidades
    # Puedes usar bibliotecas como psutil o consultar el sistema operativo
    return False  # Devuelve True si el productor está en ejecución, False de lo contrario


def is_second_producer_running():
    # Lógica para verificar si el segundo productor ya está en ejecución
    # Puedes ajustar esta lógica según tus necesidades
    # Puedes usar bibliotecas como psutil o consultar el sistema operativo
    return False  # Devuelve True si el segundo productor está en ejecución, False de lo contrario


if __name__ == '__main__':
    socketio.start_background_task(target=background_thread)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)

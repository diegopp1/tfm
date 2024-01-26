from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaException
from pymongo.mongo_client import MongoClient
import json
import logging
import subprocess
from decouple import config
import plotly.express as px

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
    'auto.offset.reset': 'latest'
})

try:
    consumer.subscribe(['locations', 'datos'])
except KafkaException as e:
    logger.error("Error al suscribirse a los temas de Kafka:", e)
    raise SystemExit("No se pudo suscribir a los temas de Kafka. Saliendo...")

devices_by_location = {}
selected_country = 'US'

def consume_message():
    try:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            topic = msg.topic()
            data = json.loads(msg.value().decode('utf-8'))
            if isinstance(data, dict):
                logger.info("Datos recibidos del tema {}: {}".format(topic, data))
                data['_topic'] = topic  # Añadir el topic al diccionario
                return data
            else:
                logger.warning("Datos no válidos recibidos del tema {}: {}".format(topic, data))
    except Exception as e:
        logger.error(f"Error al procesar mensaje de Kafka: {e}")
    return None

def generate_filtered_device(data):
    return {
        'id': data.get('id'),
        'name': data.get('name'),
        'country': data.get('country'),
        'lastUpdated': data.get('lastUpdated'),
        'parameters': [
            param for param in data.get('parameters', [])
            if param.get('id') in [1, 2, 135]  # Filtrar por ID 1, 2 y 135
        ]
    }

def generate_filtered_data(data):
    return {
        'id': data.get('id'),
        'name': data.get('name'),
        'country': data.get('country'),
        'lastUpdated': data.get('lastUpdated'),
        'parameters': [
            param for param in data.get('parameters', [])
            if param.get('id') in [1, 2, 135]  # Filtrar por ID 1, 2 y 135
        ]
    }

def background_thread():
    while True:
        data = consume_message()
        if data is not None:
            if '_id' in data:
                data['_id'] = str(data['_id'])
            socketio.emit('air_quality_data', data)
            handle_devices(data)
            socketio.sleep(1)

@app.route('/graph')
def graph():
    # Obtener los campos disponibles para los ejes X e Y
    available_fields = get_available_fields()
    return render_template('graph.html', available_fields=available_fields)

@app.route('/get_graph_data', methods=['POST'])
def get_graph_data():
    x_axis_field = request.form.get('x-axis-field')
    y_axis_field = request.form.get('y-axis-field')

    # Lógica para obtener los datos necesarios desde tu base de datos (MongoDB en este caso)

    # Ejemplo (suponiendo que mongo_air_quality_collection es tu colección MongoDB):
    data = mongo_locations_collection.find({}, {x_axis_field: 1, y_axis_field: 1, 'parameters': 1, '_id': 0})
    data_list = list(data)

    return jsonify(data_list)
def get_available_fields():
    # Obtener un documento de la colección como ejemplo
    example_document = mongo_locations_collection.find_one()

    if example_document:
        # Obtener los nombres de los campos
        field_names = ['name', 'lastUpdated']  # Campos comunes
        if 'parameters' in example_document:
            # Si hay un campo 'parameters', agregar los campos dentro de ese campo
            parameter_fields = example_document['parameters'][0].keys()
            field_names.extend(parameter_fields)

        return field_names

    return []
@app.route('/')
def index():
    global producer_running
    if not producer_running:
        start_producer()
        producer_running = True
    return render_template('index2.html')

@app.route('/locations')
def locations():
    return render_template('locations.html', country=selected_country)

@app.route('/generate', methods=['POST'])
def generate_data():
    global selected_country
    selected_country = request.form.get('country')
    start_second_producer(selected_country)
    print ('Conectado')
    openaq_data_loc = consume_message()
    if openaq_data_loc is not None:
        for entry in openaq_data_loc:
            if '_id' in entry:
                entry['_id'] = str(entry['_id'])
        handle_devices(entry)
        print("Datos de calidad del aire insertados en MongoDB.")
    return 'Generating data...'

@app.route('/data')
def data():
    stored_data = list(mongo_locations_collection.find())
    return render_template('data.html', stored_data=stored_data)

def handle_devices(data):
    id = data.get('id')
    country = data.get('country')
    topic = data.get('_topic')  # Obtener el topic

    if topic == 'locations':
        # Manejar los datos de 'locations'
        if id not in devices_by_location:
            devices_by_location[id] = {
                'id': id,
                'country': country,
                'devices': []
            }

        devices = devices_by_location[id]['devices']
        filtered_device = generate_filtered_device(data)
        devices.append(filtered_device)
        mongo_locations_collection.insert_one(filtered_device)
        socketio.emit('devices', [filtered_device])

    elif topic == 'datos':
        # Manejar los datos de 'datos'
        data['_id'] = str(data.get('_id', ''))  # Convertir el _id a string
        filtered_data = generate_filtered_data(data)

        # Solo insertar y emitir si hay parámetros después del filtrado
        if filtered_data['parameters']:
            mongo_air_quality_collection.insert_one(filtered_data)
            socketio.emit('air_quality_data', json.dumps(filtered_data))

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
    if not is_second_producer_running():
        script_path = 'C:\\Users\\Usuario\\PycharmProjects\\pythonProject2\\Producer (OpenAQ)\\kafka-producer-loc.py'
        subprocess.Popen(['python', script_path, country])
        print("Segundo productor de Kafka iniciado.")
    else:
        print("El segundo productor ya está en ejecución.")

def is_producer_running():
    return False

def is_second_producer_running():
    return False

if __name__ == '__main__':
    socketio.start_background_task(target=background_thread)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)

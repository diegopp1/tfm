from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaException
from pymongo.mongo_client import MongoClient
import json
import logging
import subprocess
from decouple import config
import plotly.express as px
from statistics import mean

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

def get_available_fields():
    # Devolver las opciones específicas para los ejes X e Y
    return ['name', 'lastUpdated', 'country', 'lastValue(pm10)', 'lastValue(pm25)', 'lastValue(um100)']

@app.route('/graph')
def graph():
    # Obtener los campos disponibles para los ejes X e Y
    available_fields = get_available_fields()
    return render_template('graph.html', available_fields=available_fields)

@app.route('/get_graph_data', methods=['POST'])
def get_graph_data():
    x_axis_field = request.form.get('x-axis-field')
    y_axis_field = request.form.get('y-axis-field')

    # Lista de parámetros que quieres incluir en la gráfica
    y_parameters = ['pm10', 'pm25', 'um100']
    print(f"Selected X-axis: {x_axis_field}, Y-axis: {y_axis_field}")
    # Si el eje X es 'country' y el eje Y es pm10, pm25 o um100
    if x_axis_field == 'country' and any(y_axis_field.endswith(f'({param})') for param in y_parameters):
        averages_by_country = calculate_average_by_country(mongo_locations_collection)
        print(averages_by_country)
        return jsonify(averages_by_country)

    # Si el eje X es 'name', proceder con la lógica original
    elif x_axis_field == 'name':
        # Ajustar la consulta para incluir solo 'name' y 'parameters' en la proyección
        data_cursor = mongo_locations_collection.find(
            {'name': {"$exists": True}, 'parameters': {"$exists": True}},
            {'name': 1, 'parameters': 1, '_id': 0}
        )

        data_list = []
        for entry in data_cursor:
            try:
                # Crear un nuevo diccionario con los valores específicos
                new_entry = {'name': entry.get('name')}

                # Agregar los valores de cada parámetro al diccionario
                for param in y_parameters:
                    value = next((p.get('lastValue', 0) for p in entry.get('parameters', []) if p.get('parameter') == param), 0)
                    new_entry[f'lastValue({param})'] = value

                data_list.append(new_entry)
            except KeyError as e:
                print(f"KeyError: {e} - Ignorando la entrada sin el valor correspondiente.")

        return jsonify(data_list)

    # Manejar otros casos o devolver un mensaje de error si es necesario
    else:
        return jsonify({'error': 'Invalid request. Please select valid axes.'})

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
@app.route('/worldmap')
def world_map():
    return render_template('map.html')

@app.route('/get_map_data', methods=['GET'])
def get_map_data():
    averages_with_coordinates = get_averages_with_coordinates(mongo_locations_collection)
    return jsonify(averages_with_coordinates)

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
def calculate_average_by_country(collection):
    # Obtener la lista de países únicos presentes en la colección
    unique_countries = collection.distinct('country')

    # Inicializar un diccionario para almacenar las medias por país
    averages_by_country = {}

    # Iterar sobre cada país y calcular la media para pm10, pm25 y um100
    for country in unique_countries:
        country_data = collection.find({'country': country}, {'parameters': 1, '_id': 0})

        # Inicializar listas para almacenar valores de pm10, pm25 y um100 para el país actual
        pm10_values = []
        pm25_values = []
        um100_values = []

        # Iterar sobre los documentos del país actual
        for entry in country_data:
            for param in entry.get('parameters', []):
                parameter = param.get('parameter')
                last_value = param.get('lastValue', 0)

                # Almacenar valores en las listas correspondientes
                if parameter == 'pm10':
                    pm10_values.append(last_value)
                elif parameter == 'pm25':
                    pm25_values.append(last_value)
                elif parameter == 'um100':
                    um100_values.append(last_value)

        # Calcular la media para pm10, pm25 y um100
        pm10_average = mean(pm10_values) if pm10_values else 0
        pm25_average = mean(pm25_values) if pm25_values else 0
        um100_average = mean(um100_values) if um100_values else 0

        # Almacenar las medias en el diccionario
        averages_by_country[country] = {
            'pm10_average': pm10_average,
            'pm25_average': pm25_average,
            'um100_average': um100_average
        }

    return averages_by_country


def get_averages_with_coordinates(collection):
    averages_by_country = calculate_average_by_country(collection)

    # Agregar coordenadas a las medias por país
    averages_with_coordinates = {}
    for country, averages in averages_by_country.items():
        country_data = collection.find({'country': country, 'coordinates': {'$exists': True}},
                                       {'coordinates': 1, '_id': 0})
        coordinates = country_data[0].get('coordinates', {}) if country_data.count() > 0 else {}

        averages_with_coordinates[country] = {
            'pm10_average': averages['pm10_average'],
            'pm25_average': averages['pm25_average'],
            'um100_average': averages['um100_average'],
            'location': {'lat': coordinates.get('latitude', 0), 'lon': coordinates.get('longitude', 0)}
        }

    return averages_with_coordinates

if __name__ == '__main__':
    socketio.start_background_task(target=background_thread)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)


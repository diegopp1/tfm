import time
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaException
from pymongo.mongo_client import MongoClient
import json
import logging
import subprocess
from decouple import config
from bson import json_util
from statistics import mean
import os

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
mongo_countries_collection = mongo_db['countries']
mongo_sensors_collection = mongo_db['my_sensors']
mongo_sensors_data_collection = mongo_db['my_sensors_data']
# Crear el consumidor de Kafka
consumer = Consumer({
    'bootstrap.servers': 'broker:29092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
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
        ],
        'coordinates': data.get('coordinates')
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
        ],
        'coordinates': data.get('coordinates')
    }

def background_thread():
    try:
        while True:
            data = consume_message()
            if data is not None:
                if '_id' in data:
                    data['_id'] = str(data['_id'])
                socketio.emit('air_quality_data', data)
                handle_devices(data)
                socketio.sleep(1)
    except Exception as e:
        logger.error(f"Error en el hilo principal: {e}")

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
    try:
        global selected_country
        selected_country = request.form.get('country')
        start_second_producer(selected_country)
        print('Conectado')
        return redirect(url_for('index'))
    except Exception as e:
        # Manejar la excepción aquí, puedes registrarla o hacer algo más según tus necesidades
        print(f"Error en la ruta '/generate': {e}")
        return jsonify({'error': 'Se produjo un error al procesar la solicitud'})

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


@app.route('/perform_search', methods=['POST'])
def perform_search():
    search_input = request.json.get('searchInput', '')

    # Realiza la búsqueda en la base de datos según el campo 'country'
    filtered_data = list(mongo_locations_collection.find({'country': search_input}))

    # Convertir ObjectId a cadena antes de serializar a JSON
    filtered_data = json.loads(json_util.dumps(filtered_data))

    return jsonify(filtered_data)

@app.route('/add_sensor_to_list', methods=['POST'])
def add_sensor_to_list():
    try:
        sensor_data = request.json
        sensor_id = sensor_data.get('sensorId')
        location = sensor_data.get('location')
        country = sensor_data.get('country')

        # Verificar si el sensor ya está en la lista
        existing_sensor = mongo_sensors_collection.find_one({'_id': sensor_id})

        if existing_sensor:
            return jsonify({'error': 'El sensor ya está en la lista.'}), 400

        # Si no existe, agregar el sensor a la lista
        new_sensor = {
            '_id': sensor_id,
            'location': location,
            'country': country
        }

        mongo_sensors_collection.insert_one(new_sensor)

        return jsonify({'message': 'Sensor agregado correctamente a la lista.'}), 200
    except Exception as e:
        print(f"Error al agregar el sensor: {e}")
        return jsonify({'error': 'Error interno del servidor.'}), 500

@app.route('/my_sensors')
def my_sensors():
    sensors_list = list(mongo_sensors_collection.find())
    return render_template('my_sensors.html', sensors_list=sensors_list)

@app.route('/remove_sensor', methods=['POST'])
def remove_sensor():
    try:
        sensor_data = request.json
        sensor_id = sensor_data.get('sensorId')

        # Eliminar el sensor de la lista
        mongo_sensors_collection.delete_one({'_id': sensor_id})

        return jsonify({'message': 'Sensor eliminado correctamente.'}), 200
    except Exception as e:
        print(f"Error al eliminar el sensor: {e}")
        return jsonify({'error': 'Error interno del servidor.'}), 500

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

def get_project_root() -> str:
    """Obtiene la ruta al directorio principal del proyecto."""
    return os.path.dirname(os.path.abspath(__file__))

def start_producer():
    if not is_producer_running():
        script_path = '/app/Producer (OpenAQ)/kafka-producerv3.py'
        subprocess.Popen(['python', script_path])
        print("Productor de Kafka iniciado.")
    else:
        print("El productor ya está en ejecución.")

def start_second_producer(country):
    if not is_second_producer_running():
        script_path = '/app/Producer (OpenAQ)/kafka-producer-loc.py'
        subprocess.Popen(['python', script_path, country])
        print("Segundo productor de Kafka iniciado.")
    else:
        print("El segundo productor ya está en ejecución.")

def is_producer_running():
    return False

def is_second_producer_running():
    return False

def start_sensors_producer(sensor_id):
    try:
        # Obtén la información del sensor desde MongoDB
        sensor_info = mongo_sensors_collection.find_one({"_id": sensor_id})

        if sensor_info is not None:
            # Configuración del productor de Kafka
            kafka_producer_script = "/app/Producer (OpenAQ)/kafka-producer-mysensors.py"

            # Iniciar el proceso del productor con los argumentos necesarios
            subprocess.Popen(["python", kafka_producer_script, sensor_info["_id"]])
            time.sleep(5)
            # Suscribirse al tema 'my_sensors' para recibir los datos del sensor
            consumer.subscribe('my_sensors')
            time.sleep(2)
            data = consume_message()
            # Agregar una entrada en la nueva colección de MongoDB para el sensor
            data_filtered = generate_filtered_data(data)

            mongo_sensors_data_collection.insert_one(data_filtered)
            return {"status": "success", "message": f"Productor iniciado para el sensor {sensor_id}"}
        else:
            return {"status": "error", "message": f"No se encontró información para el sensor {sensor_id}"}

    except Exception as e:
        return {"status": "error", "message": f"Error al iniciar el productor: {str(e)}"}
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

# ...

def get_averages_with_coordinates(collection):
    # Obtener la información de coordenadas de cada país desde la colección 'countries'
    country_coordinates = {}
    for country_info in mongo_countries_collection.find({}, {'country': 1, 'coordinates': 1, '_id': 0}):
        country_code = country_info['country']
        coordinates = country_info.get('coordinates', {})
        latitude = coordinates.get('latitude')
        longitude = coordinates.get('longitude')
        if latitude is not None and longitude is not None:
            country_coordinates[country_code] = coordinates
        else:
            print(f'Documento de país sin coordenadas válidas en la colección "countries": {country_info}')

    averages_by_country = calculate_average_by_country(collection)

    # Agregar coordenadas a las medias por país
    averages_with_coordinates = {}
    for country, averages in averages_by_country.items():
        coordinates = country_coordinates.get(country, {})
        if coordinates:
            print(f'Coordenadas encontradas para el país {country}: {coordinates}')
            print(f'Averages para el país {country}: {averages}')

            # Agregar todas las medias con coordenadas, independientemente de los umbrales
            averages_with_coordinates[country] = {
                'pm10_average': averages['pm10_average'],
                'pm25_average': averages['pm25_average'],
                'um100_average': averages['um100_average'],
                'location': {'lat': coordinates.get('latitude'), 'lon': coordinates.get('longitude')}
            }
        else:
            print(f'No se encontraron coordenadas para el país {country} en la colección "countries".')
    print(f'Averages with coordinates: {averages_with_coordinates}')
    return averages_with_coordinates

if __name__ == '__main__':
    socketio.start_background_task(target=background_thread) # Iniciar el hilo de fondo para consumir datos de Kafka
    socketio.run(app,  host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)
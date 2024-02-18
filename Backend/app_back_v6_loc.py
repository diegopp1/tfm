import time
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, Response
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
from functools import wraps
app = Flask(__name__)
socketio = SocketIO(app, threaded=True)

producer_running = False
sec_producer_running = False

logging.basicConfig(level=logging.INFO)
cons_logger = logging.getLogger('consumer')
app_logger = logging.getLogger('app_back_v6_loc')

# Configuración de MongoDB
mongo_password = config('MONGO_PASSWORD', default='')
mongo_uri = f"mongodb+srv://diegopp1:{mongo_password}@cluster0.omhmfeu.mongodb.net/?retryWrites=true&w=majority"
mongo_client = MongoClient(mongo_uri)

try:
    # Verificación de la conexión a MongoDB al iniciar la aplicación
    mongo_client.admin.command('ping')
    cons_logger.info("Conexión a MongoDB establecida exitosamente!")
except Exception as e:
    cons_logger.error("Error conectando a MongoDB:", e)
    raise SystemExit("No se pudo conectar a MongoDB. Saliendo...")

mongo_db = mongo_client['iot_data']
mongo_air_quality_collection = mongo_db['air_quality']
mongo_locations_collection = mongo_db['locations']
mongo_countries_collection = mongo_db['countries']
mongo_sensors_collection = mongo_db['my_sensors']
mongo_sensors_data_collection = mongo_db['my_sensors_data']

# Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': 'broker:29092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'latest'
})

try:
    consumer.subscribe(['locations', 'datos', 'my_sensors'])
except KafkaException as e:
    cons_logger.error("Error al suscribirse a los temas de Kafka:", e)
    raise SystemExit("No se pudo suscribir a los temas de Kafka. Saliendo...")

devices_by_location = {}
selected_country = 'US'

# Hardcoded credentials for the single user
USERNAME = 'admin'
PASSWORD = 'admin'

# Function to check if the provided credentials are correct
def check_credentials(username, password):
    return username == USERNAME and password == PASSWORD

# Define your require_authentication decorator function
def require_authentication(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_credentials(auth.username, auth.password):
            return Response('Authentication required', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})
        return func(*args, **kwargs)
    return decorated

@app.route('/')
@require_authentication
def index():
    global producer_running
    if not producer_running:
        start_producer()
        producer_running = True

    latest_sensor = mongo_air_quality_collection.find_one({}, sort=[('_id', -1)])
    if 'parameters' in latest_sensor:
        # Convert lastUpdated to a more readable format
        for parameter in latest_sensor['parameters']:
            if 'lastUpdated' in parameter:
                parameter['lastUpdated'] = datetime.strptime(parameter['lastUpdated'], '%Y-%m-%dT%H:%M:%S%z').strftime(
                    '%Y-%m-%d %H:%M:%S')

    return render_template('index2.html', latest_sensor=latest_sensor)


@app.route('/locations')
def locations():
    return render_template('locations.html', country=selected_country)


@app.route('/generate', methods=['POST'])
def generate_data():
    try:
        global selected_country
        selected_country = request.form.get('country')
        start_second_producer(selected_country)
        app_logger.info('Conectado')
        time.sleep(10)
        return redirect(url_for('index'))
    except Exception as e:
        app_logger.info(f"Error en la ruta '/generate': {e}")
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
    # Eliminating duplicates and displaying the data
    pipeline = [
        {
            '$group': {
                '_id': {
                    'name': '$name',
                    'country': '$country',
                    'lastUpdated': '$lastUpdated'
                },
                'document': {'$first': '$$ROOT'}
            }
        },
        {
            '$replaceRoot': {'newRoot': '$document'}
        }
    ]

    try:
        result = list(mongo_locations_collection.aggregate(pipeline))
        app_logger.info(f"Number of documents after aggregation: {len(result)}")
        diff = len(mongo_locations_collection.distinct('_id')) - len(result)
        app_logger.warning(f"Number of duplicates to be removed: {diff}")
        # Verificar si hay duplicados antes de eliminar e insertar
        if diff > 0:
            mongo_locations_collection.delete_many({})
            mongo_locations_collection.insert_many(result)
            app_logger.warning("Duplicates removed and inserted.")
        else:
            app_logger.info("No duplicates found.")
    except Exception as e:
        app_logger.error(f"Error during aggregation: {e}")
        return jsonify(error=str(e))

    return render_template('data.html', stored_data=result)
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
        app_logger.error(f"Error al agregar el sensor: {e}")
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
        mongo_sensors_collection.delete_one({'_id': sensor_id})
        return jsonify({'message': 'Sensor eliminado correctamente.'}), 200
    except Exception as e:
        app_logger.error(f"Error al eliminar el sensor: {e}")
        return jsonify({'error': 'Error interno del servidor.'}), 500

@app.route('/graph_mysensors', methods=['GET', 'POST'])
def graph_mysensors():
    if request.method == 'POST':
        y_axis_field = request.form.get('y-axis-field')

        # Lista de parámetros que quieres incluir en la gráfica
        y_parameters = ['pm10', 'pm25', 'o3', 'no2', 'so2', 'co']

        # Obtener los datos relevantes de la colección
        data_cursor = mongo_sensors_data_collection.find({}, {'location': 1, 'parameters': 1, '_id': 0})
        data_list = []

        for entry in data_cursor:
            try:
                # Creating a new dictionary to store the data
                new_entry = {'location': entry.get('location')}

                # Obtaining the last updated date for each sensor
                last_updated_dates = [datetime.strptime(param.get('lastUpdated', ''), '%Y-%m-%dT%H:%M:%S%z')
                                      for param in entry.get('parameters', []) if param.get('id') in y_parameters]

                new_entry['lastUpdated'] = max(last_updated_dates).strftime('%Y-%m-%d %H:%M:%S')

                # Aggregating the values for each parameter
                for param_id in y_parameters:
                    value = next(
                        (param.get('value') for param in entry.get('parameters', []) if param.get('id') == param_id), 0)
                    new_entry[param_id] = value

                data_list.append(new_entry)
            except KeyError as e:
                print(f"KeyError: {e} - Ignorando la entrada sin el valor correspondiente.")

        return render_template('sensor_chart.html', data_list=data_list)

    return render_template('sensor_chart.html')
@app.route('/graph')
def graph():
    # Get available fields from the database
    available_fields = get_available_fields()
    return render_template('graph.html', available_fields=available_fields)
@app.route('/get_graph_data', methods=['POST'])
def get_graph_data():
    x_axis_field = request.form.get('x-axis-field')
    y_axis_field = request.form.get('y-axis-field')
    # List of parameters to include in the graph
    y_parameters = ['pm10', 'pm25', 'o3', 'no2', 'so2', 'co']
    print(f"Selected X-axis: {x_axis_field}, Y-axis: {y_axis_field}")
    # If the X-axis is 'country' and the Y-axis is pm10, pm25, o3, no2, so2 or co, return the average value for each 'country'
    if x_axis_field == 'country' and any(y_axis_field.endswith(f'({param})') for param in y_parameters):
        averages_by_country = calculate_average_by_country(mongo_locations_collection)
        print(averages_by_country)
        return jsonify(averages_by_country)
    # If the X-axis is 'name' and the Y-axis is pm10, pm25, o3, no2, so2 or co, return the latest value for each 'name'
    elif x_axis_field == 'name':
        # Modify the query to sort by 'lastUpdated' within parameters and return the latest entry for each 'name'
        pipeline = [
            {"$match": {"name": {"$exists": True}}},
            {"$unwind": "$parameters"},
            {"$match": {"parameters.parameter": {"$in": y_parameters}}},
            {"$sort": {"parameters.lastUpdated": -1}},
            {"$group": {
                "_id": {"name": "$name", "parameter": "$parameters.parameter"},
                "lastValue": {"$first": "$parameters.lastValue"}
            }},
            {"$group": {
                "_id": "$_id.name",
                "parameters": {
                    "$push": {
                        "parameter": "$_id.parameter",
                        "lastValue": "$lastValue"
                    }
                }
            }},
            {"$project": {
                "name": "$_id",
                "parameters": 1,
                "_id": 0
            }}
        ]
        data_cursor = mongo_locations_collection.aggregate(pipeline)
        data_list = []
        for entry in data_cursor:
            new_entry = {'name': entry['name']}
            # Convert the parameters list to a dictionary for easy access
            params_dict = {p['parameter']: p['lastValue'] for p in entry['parameters']}
            # Now you can directly get the last value using the parameter name
            for param in y_parameters:
                param_key = f"lastValue({param})"
                new_entry[param_key] = params_dict.get(param, 0)
            data_list.append(new_entry)
        return jsonify(data_list)
    else:
        return jsonify({'error': 'Invalid request. Please select valid axes.'})
@app.route('/sensor_chart')
def sensor_chart():
    # Get the list of sensors from the database
    sensors_list = list(mongo_sensors_collection.find({}, {'_id': 1, 'location': 1, 'country': 1}))
    return render_template('sensor_chart.html', sensors_list=sensors_list)

@app.route('/generate_sensor_chart', methods=['POST'])
def generate_sensor_chart():
    location = request.form.get('sensor')
    parameter_id = request.form.get('parameter')
    print(f"Sensor: {location}, Parameter: {parameter_id}")
    # Getting the data for the selected sensor and parameter
    data_cursor = mongo_sensors_data_collection.find(
        {'location': location, 'parameters.id': parameter_id},
        {'parameters.$': 1, '_id': 0}
    ).sort('parameters.lastUpdated', 1)
    print(data_cursor)
    data_list = []
    # Extracting the lastUpdated and value for each entry
    for entry in data_cursor:
        parameter = entry['parameters'][0]
        value = parameter.get('value', 0)
        last_updated = parameter.get('lastUpdated', 0)
        data_list.append({'lastUpdated': last_updated, 'value': value})

    return jsonify(data_list)
def consume_message():
    try:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            topic = msg.topic() # Get the topic of the message
            data = json.loads(msg.value().decode('utf-8'))
            if isinstance(data, dict):
                cons_logger.info("Datos recibidos del tema {}: {}".format(topic, data))
                data['_topic'] = topic  # Añadir el topic al diccionario
                return data # Required to commit the message
            else:
                cons_logger.warning("Datos no válidos recibidos del tema {}: {}".format(topic, data))
    except Exception as e:
        cons_logger.error(f"Error al procesar mensaje de Kafka: {e}")
    return None


def generate_filtered_device(data):
    return {
        'id': data.get('id'),
        'name': data.get('name'),
        'country': data.get('country'),
        'lastUpdated': data.get('lastUpdated'),
        'parameters': [
            param for param in data.get('parameters', [])
            if param.get('id') in [1, 2, 3, 4, 5, 6]  # Filtrar por ID 1, 2, 3, 4, 5, 6
        ],
        'coordinates': data.get('coordinates')
    }


def generate_filtered_data(data):
    return {
        'location': data.get('location'),
        'country': data.get('country'),
        'parameters': [
            {
                'id': param.get('parameter'),  # Usar 'parameter' en lugar de 'id' si es el campo correcto
                'value': param.get('value'),
                'unit': param.get('unit'),
                'lastUpdated': param.get('lastUpdated')
            }
            for param in data.get('measurements', [])  # Ajustar al nombre correcto de los datos de 'my_sensors'
        ],
        'coordinates': data.get('coordinates')
    }
def background_thread():
    try:
        while True:
            data = consume_message()
            if data is not None:
                handle_devices(data)
                socketio.sleep(1)
    except Exception as e:
        cons_logger.error(f"Error en el hilo principal: {e}")


def get_available_fields():
    # Return the list of available fields for the sensors
    return ['name', 'lastUpdated', 'country', 'lastValue(pm10)', 'lastValue(pm25)', 'lastValue(o3)', 'lastValue(no2)', 'lastValue(so2)', 'lastValue(co)']
def handle_devices(data):
    topic = data.get('_topic')  # Obtener el topic
    app_logger.info(f"Manejando datos de '{topic}'")
    if topic == 'locations':
        # Manage the data from 'locations'
        filtered_device = generate_filtered_device(data)  # Filtrar los datos del dispositivo
        mongo_locations_collection.insert_one(filtered_device)

    elif topic == 'datos':
        # Manage the data from 'datos'
        filtered_data = generate_filtered_data(data)
        # Solo insertar y emitir si hay parámetros después del filtrado
        mongo_air_quality_collection.insert_one(filtered_data)

    elif topic == 'my_sensors':
        # Manage the data from 'my_sensors'
        filtered_sensor = generate_filtered_data(data)
        mongo_sensors_data_collection.insert_one(filtered_sensor)


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
        app_logger.info("Productor de Kafka iniciado.")
    else:
        app_logger.info("El productor ya está en ejecución.")
def start_second_producer(country):
    if not is_second_producer_running():
        script_path = '/app/Producer (OpenAQ)/kafka-producer-loc.py'
        subprocess.Popen(['python', script_path, country])
        app_logger.info("Segundo productor de Kafka iniciado.")
    else:
        app_logger.info("El segundo productor ya está en ejecución.")
def is_producer_running():
    return False


def is_second_producer_running():
    return False
@app.route('/start_mysensors_producer', methods=['POST'])
def mysensors_produce():
    # Gets the sensor ID from the request
    sensor_id = request.get_json().get('sensor_id')

    try:
        # Get the sensor information from the database
        sensor_info = mongo_sensors_collection.find_one({"_id": sensor_id})

        if sensor_info is not None:
            # Kafka producer script path
            kafka_producer_script = "/app/Producer (OpenAQ)/kafka-producer-mysensors.py"
            subprocess.Popen(["python", kafka_producer_script, sensor_info['_id']])
            app_logger.info(f"Productor de Kafka iniciado para el sensor {sensor_info['_id']}")
            cons_logger.info(f"Consumidor suscrito al tema 'my_sensors' para el sensor {sensor_info['_id']}")
        else:
            app_logger.error(f"No se encontró el sensor con el ID {sensor_id}")

    except Exception as e:
        app_logger.error(f"Error al iniciar el productor de Kafka: {e}")

    return redirect(url_for('my_sensors'))


def calculate_average_by_country(collection):
    # Get the unique countries from the collection
    unique_countries = collection.distinct('country')

    # Initialize an empty dictionary to store the averages by country
    averages_by_country = {}

    # Iterate over the unique countries
    for country in unique_countries:
        country_data = collection.find({'country': country}, {'parameters': 1, '_id': 0})

        # Inicializar listas para almacenar valores de pm10, pm25, o3, no2, so2 y co para el país actual
        pm10_values = []
        pm25_values = []
        o3_values = []
        no2_values = []
        so2_values = []
        co_values = []

        # Iterate over the country data
        for entry in country_data:
            for param in entry.get('parameters', []):
                parameter = param.get('parameter')
                last_value = param.get('lastValue', 0)
                # Save the last value for each parameter
                if parameter == 'pm10':
                    pm10_values.append(last_value)
                elif parameter == 'pm25':
                    pm25_values.append(last_value)
                elif parameter == 'o3':
                    o3_values.append(last_value)
                elif parameter == 'no2':
                    no2_values.append(last_value)
                elif parameter == 'so2':
                    so2_values.append(last_value)
                elif parameter == 'co':
                    co_values.append(last_value)

        # Calculate the average for each parameter
        pm10_average = mean(pm10_values) if pm10_values else 0
        pm25_average = mean(pm25_values) if pm25_values else 0
        o3_average = mean(o3_values) if o3_values else 0
        no2_average = mean(no2_values) if no2_values else 0
        so2_average = mean(so2_values) if so2_values else 0
        co_average = mean(co_values) if co_values else 0

        # Save the averages for the current country
        averages_by_country[country] = {
            'pm10_average': pm10_average,
            'pm25_average': pm25_average,
            'o3_average': o3_average,
            'no2_average': no2_average,
            'so2_average': so2_average,
            'co_average': co_average
        }

    return averages_by_country


def get_averages_with_coordinates(collection):
    # Get the coordinates for each country
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

    # Aggregating the averages with the coordinates
    averages_with_coordinates = {}
    for country, averages in averages_by_country.items():
        coordinates = country_coordinates.get(country, {})
        if coordinates:
            print(f'Coordenadas encontradas para el país {country}: {coordinates}')
            print(f'Averages para el país {country}: {averages}')
            # Aggregating the averages with the coordinates
            averages_with_coordinates[country] = {
                'pm10_average': averages['pm10_average'],
                'pm25_average': averages['pm25_average'],
                'o3_average': averages['o3_average'],
                'no2_average': averages['no2_average'],
                'so2_average': averages['so2_average'],
                'co_average': averages['co_average'],
                'location': {'lat': coordinates.get('latitude'), 'lon': coordinates.get('longitude')}
            }
        else:
            print(f'No se encontraron coordenadas para el país {country} en la colección "countries".')
    print(f'Averages with coordinates: {averages_with_coordinates}')
    return averages_with_coordinates


if __name__ == '__main__':
    socketio.start_background_task(target=background_thread)  # Initialize the background thread for getting data from Kafka
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)

import requests
from pymongo import MongoClient
from decouple import config

# Configuración de MongoDB
mongo_password = config('MONGO_PASSWORD', default='')
mongo_uri = f"mongodb+srv://diegopp1:{mongo_password}@cluster0.omhmfeu.mongodb.net/?retryWrites=true&w=majority"
mongo_client = MongoClient(mongo_uri)

try:
    # Verificación de la conexión a MongoDB al iniciar la aplicación
    mongo_client.admin.command('ping')
    print("Conexión a MongoDB establecida exitosamente!")
except Exception as e:
    print("Error conectando a MongoDB:", e)
    raise SystemExit("No se pudo conectar a MongoDB. Saliendo...")

mongo_db = mongo_client['iot_data']
mongo_data_2019_collection = mongo_db['data_2019']

# URL base de la API de OpenAQ para obtener datos de calidad del aire
openaq_base_url = "https://api.openaq.org/v2/measurements"

# Umbrales específicos
thresholds = {
    'pm10': 100,
    'pm25': 400,
    'um100': 0.03
}

# Rango de fechas
date_from = '2019-01-01T00:00:00'
date_to = '2019-12-31T23:59:00'

# Países de interés
countries = ['AR', 'CA', 'CL', 'DE', 'FR', 'IT', 'US', 'ES', 'PT', 'CN', 'JP', 'BR', 'RU', 'IN', 'AU', 'ZA']

# Iterar sobre los umbrales y realizar solicitudes para cada uno
for param, threshold in thresholds.items():
    params = {
        'date_from': date_from,
        'date_to': date_to,
        'limit': 10,  # Aumenta el límite para obtener más datos
        'page': 1,
        'offset': 0,
        'sort': 'desc',
        'parameter': param,
        'radius': 1000,
        'country': countries,
        'value_from': threshold,
        'order_by': 'datetime'
    }
    try:
        response = requests.get(openaq_base_url, params=params)

        if response.status_code == 200:
            openaq_data = response.json()['results']
            # Almacena los datos en la colección de MongoDB
            mongo_data_2019_collection.insert_many(openaq_data)
            print(f"Datos almacenados en la colección 'data_2019' para el parámetro {param} y umbral {threshold}")
        else:
            print(f"Error al obtener datos de calidad del aire de OpenAQ. Código de estado: {response.status_code}")
    except Exception as e:
        print(f"Error en la solicitud a la API de OpenAQ: {e}")

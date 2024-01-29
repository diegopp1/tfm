from pymongo import MongoClient
from decouple import config

# Configuración de MongoDB
mongo_password = config('MONGO_PASSWORD', default='')
mongo_uri = f"mongodb+srv://diegopp1:{mongo_password}@cluster0.omhmfeu.mongodb.net/?retryWrites=true&w=majority"
mongo_client = MongoClient(mongo_uri)

# Base de datos y colecciones
mongo_db = mongo_client['iot_data']
mongo_locations_collection = mongo_db['locations']
mongo_countries_collection = mongo_db['countries']

# Datos de coordenadas de capitales
capitals_coordinates = {
    'AR': {'latitude': -34.61, 'longitude': -58.38},  # Buenos Aires
    'CA': {'latitude': 45.42, 'longitude': -75.69},   # Ottawa
    'CL': {'latitude': -33.45, 'longitude': -70.65},  # Santiago
    'DE': {'latitude': 52.52, 'longitude': 13.41},    # Berlín
    'FR': {'latitude': 48.86, 'longitude': 2.35},     # París
    'IT': {'latitude': 41.89, 'longitude': 12.49},    # Roma
    'US': {'latitude': 38.90, 'longitude': -77.04},   # Washington, D.C.
    'ES': {'latitude': 40.42, 'longitude': -3.70},    # Madrid
    'PT': {'latitude': 38.72, 'longitude': -9.14},    # Lisboa
    'CN': {'latitude': 39.90, 'longitude': 116.41},   # Pekín
    'JP': {'latitude': 35.68, 'longitude': 139.76},  # Tokio
    'BR': {'latitude': -15.78, 'longitude': -47.93}, # Brasilia
    'RU': {'latitude': 55.75, 'longitude': 37.62},   # Moscú
    'IN': {'latitude': 28.61, 'longitude': 77.23},   # Nueva Delhi
    # Añade más capitales según sea necesario
}

# Actualizar datos de coordenadas en la colección 'countries'
for country_code, coordinates in capitals_coordinates.items():
    query = {'country': country_code}
    update = {'$set': {'coordinates': coordinates}}
    mongo_countries_collection.update_one(query, update, upsert=True)

# Imprimir mensaje
print('Datos actualizados en la colección "countries".')

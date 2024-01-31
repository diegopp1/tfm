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

# URL de la API de OpenAQ para obtener datos de calidad del aire por país y fecha
openaq_data_url = "https://api.openaq.org/v2/measurements?date_from=2019-12-31T00%3A00%3A00&date_to=2019-12-31T23%3A59%3A00&limit=100&page=1&offset=0&sort=desc&parameter=pm25&parameter=pm10&parameter=um100&radius=1000&country=AR&country=CA&country=CL&country=DE&country=FR&country=IT&country=US&country=ES&country=PT&country=CN&country=JP&country=BR&country=RU&country=IN&country=AU&country=ZA&order_by=datetime"

def fetch_and_store_openaq_data():
    try:
        response = requests.get(openaq_data_url)

        if response.status_code == 200:
            openaq_data = response.json()['results']
            # Almacena los datos en la colección de MongoDB
            mongo_data_2019_collection.insert_many(openaq_data)
            print("Datos almacenados en la colección 'data_2019'")
            return True
        else:
            print(f"Error al obtener datos de calidad del aire de OpenAQ. Código de estado: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error en la solicitud a la API de OpenAQ: {e}")
        return False

if __name__ == '__main__':
    fetch_and_store_openaq_data()

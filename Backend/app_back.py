# Script for the backend of the application.
from flask import Flask, render_template, request, jsonify
import subprocess

app = Flask(__name__)

# Esta ruta define la página principal de la aplicación.
# Cuando los usuarios acceden a la ruta '/', la función 'index()' se ejecuta y muestra la plantilla 'index.html'.
@app.route('/')
def index():
    return render_template('index.html')

# Esta ruta recibe solicitudes POST a la ruta '/send-location/<location_id>'.
# La función 'send_location(location_id)' se ejecuta cuando se recibe una solicitud POST en esta ruta.
# La variable 'location_id' se extrae de la URL y se pasa como argumento a la función.
@app.route('/send-location/<location_id>', methods=['POST'])
def send_location(location_id):
    # Ejecutar el script del productor de Kafka con la ubicación proporcionada
    subprocess.run(['python', 'kafka-producer.py', location_id])
    # Responder con un JSON indicando que la operación fue exitosa.
    return jsonify({'status': 'success'})

# Esta condición verifica si el script está siendo ejecutado directamente y no importado como un módulo.
if __name__ == '__main__':
    # Iniciar la aplicación Flask en modo de depuración.
    app.run(debug=True)

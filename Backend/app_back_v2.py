# Script for the backend of the application.
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import json
import subprocess

app = Flask(__name__)
socketio = SocketIO(app)

# Ruta principal que renderiza la página web
@app.route('/')
def index():
    return render_template('index2.html')

# Ruta para iniciar la transmisión de datos al hacer clic en el botón
@app.route('/start-stream')
def start_stream():
    # Lógica para iniciar la transmisión de datos
    # Puedes agregar aquí cualquier código necesario para empezar a producir datos
    socketio.start_background_task(target=emit_data)
    return jsonify({'status': 'success'})

# Función para emitir datos a través de Socket.IO
def emit_data():
    # Aquí puedes poner la lógica para obtener datos del consumidor de Kafka externo
    # y emitirlos a través de Socket.IO
    while True:
        try:
            # Obtener datos del consumidor externo (ajusta según tu lógica)
            data = fetch_data_from_kafka_consumer()
            socketio.emit('air_quality_data', data)
            socketio.sleep(1)  # Esperar un segundo (ajusta según tu necesidad)
        except Exception as e:
            print(f"Error al obtener/enviar datos: {e}")

# Esta función simula la obtención de datos del consumidor de Kafka externo
def fetch_data_from_kafka_consumer():
    # Aquí puedes poner la lógica para obtener datos del consumidor de Kafka externo
    # Reemplaza esto con tu propia lógica
    simulated_data = {
        'location': 'Example Location',
        'value': 42,
        'unit': 'ug/m³',
    }
    return simulated_data

if __name__ == '__main__':
    socketio.run(app, debug=False, allow_unsafe_werkzeug=True)

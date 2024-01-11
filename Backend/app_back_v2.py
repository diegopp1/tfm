# Script for the backend of the application.
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import json
import requests

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
    while True:
        # Lógica para obtener datos (puedes adaptarla según tus necesidades)
        data = fetch_data()
        socketio.emit('air_quality_data', json.dumps(data))
        socketio.sleep(1)  # Esperar un segundo (ajusta según tu necesidad)

# Función de ejemplo para obtener datos (puedes adaptarla según tu caso)
def fetch_data():
    # Lógica para obtener datos, por ejemplo, de OpenAQ o cualquier otra fuente
    # Retorna un diccionario simulado, reemplaza esto con tu lógica real
    return {'location': 'Example Location', 'value': 42, 'unit': 'ug/m³'}

if __name__ == '__main__':
    socketio.run(app, debug=False, allow_unsafe_werkzeug=True)

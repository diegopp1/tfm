from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import json
from confluent_kafka import Consumer, KafkaError
from queue import Queue

app = Flask(__name__)
socketio = SocketIO(app, threaded=True)  # Configuración para permitir múltiples conexiones simultáneas

# Configuración del consumidor de Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
kafka_topic = 'topic-2'  # Cambia esto según el nombre del tema que creaste en el clúster Kafka

# Crear un consumidor
consumer = Consumer(kafka_conf)

# Suscribirse a un tema
consumer.subscribe([kafka_topic])

# Cola para pasar mensajes entre hilos
message_queue = Queue()

# Ruta principal que renderiza la página web
@app.route('/')
def index():
    return render_template('index2.html')

# Ruta para iniciar la transmisión de datos al hacer clic en el botón
@app.route('/start-stream')
def start_stream():
    # Iniciar la transmisión
    socketio.start_background_task(target=emit_data)
    return jsonify({'status': 'success'})

# Función para emitir datos a través de Socket.IO
def emit_data():
    for msg in consumer:
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # No se encontraron nuevos mensajes
                continue
            else:
                print(msg.error())
                break

        # Decodificar y procesar el mensaje JSON
        try:
            data = json.loads(msg.value().decode('utf-8'))
            message_queue.put(data)  # Poner el mensaje en la cola
        except Exception as e:
            print(f"Error al procesar mensaje: {e}")

# Función para obtener datos de la cola y emitirlos a través de Socket.IO
def background_thread():
    while True:
        data = message_queue.get()  # Obtener datos de la cola
        socketio.emit('air_quality_data', data)
        print(f"Mensaje recibido de {kafka_topic}: {data}")
        socketio.sleep(1)  # Esperar un segundo (ajusta según tu necesidad)

# Manejar la conexión del cliente
@socketio.on('connect')
def handle_connect():
    print('Cliente conectado')
    emit('status', {'data': 'Conexión establecida'})

if __name__ == '__main__':
    # Iniciar el hilo de fondo para emitir datos a través de Socket.IO
    socketio.start_background_task(target=background_thread)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)


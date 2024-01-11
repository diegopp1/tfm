from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import json
from confluent_kafka import Consumer, KafkaError

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
    try:
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
                socketio.emit('air_quality_data', data)
                print(f"Mensaje recibido de {kafka_topic}: {data}")
            except Exception as e:
                print(f"Error al procesar mensaje: {e}")

            socketio.sleep(1)  # Esperar un segundo (ajusta según tu necesidad)

    except KeyboardInterrupt:
        pass

    finally:
        # Cerrar el consumidor al salir del bucle principal
        consumer.close()

if __name__ == '__main__':
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)

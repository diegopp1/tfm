from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, KafkaError
import json
import logging
import time

app = Flask(__name__)
socketio = SocketIO(app, threaded=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
kafka_topic = 'datos'

consumer = Consumer(kafka_conf)
consumer.subscribe([kafka_topic])

@app.route('/')
def index():
    return render_template('index2.html')

@app.route('/start-stream')
def start_stream():
    time.sleep(10)
    msg = consumer.poll(1.0)
    print(type(msg))
    if msg is not None:
        try:
            data = json.loads(msg.value().decode('utf-8'))
            print(data)
            return jsonify(data)
        except Exception as e:
            logger.error(f"Error al procesar mensaje: {e}")
    else:
        return jsonify({'error': 'No hay datos'})

def consume_message():
    msg = consumer.poll(1.0)
    if msg is not None and not msg.error():
        try:
            data = json.loads(msg.value().decode('utf-8'))
            logger.info("Datos recibidos: {}".format(data))
            return data
        except Exception as e:
            logger.error(f"Error al procesar mensaje: {e}")
    return None

def background_thread():
    while True:
        data = consume_message()
        if data is not None:
            socketio.emit('air_quality_data', data)
            socketio.sleep(1)

@socketio.on('connect')
def handle_connect():
    print('Cliente conectado')
    emit('status', {'data': 'Conexión establecida'})

if __name__ == '__main__':
    socketio.start_background_task(target=background_thread)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, use_reloader=False)


# Mirar con DASH
# https://dash.plotly.com/live-updates
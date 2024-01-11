from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import subprocess
import json
app = Flask(__name__)
socketio = SocketIO(app)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/send-location/<location_id>', methods=['POST'])
def send_location(location_id):
    subprocess.run(['python', 'Producer (OpenAQ)/kafka-producer.py', location_id], cwd='..')
    return jsonify({'status': 'success'})

def kafka_consumer():
    from confluent_kafka import Consumer, KafkaError, KafkaException

    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    topic = 'json-topic'
    consumer = Consumer(conf)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                data = json.loads(msg.value().decode('utf-8'))
                print("Datos recibidos:", data)
                socketio.emit('air_quality_data', data)  # Emitir datos a través de WebSocket
            except Exception as e:
                print(f"Error al procesar mensaje: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    socketio.start_background_task(target=kafka_consumer)
    socketio.run(app, debug=True)

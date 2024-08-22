from kafka import KafkaProducer, KafkaConsumer
import json
import time

# Configuración del productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Configuración del consumidor
consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

# Enviar un mensaje de prueba al tema "test_topic"
producer.send('test_topic', {'key': 'value'})
producer.flush()

# Consumir mensajes del tema "test_topic"
for message in consumer:
    print(f"Mensaje recibido: {message.value}")
    break

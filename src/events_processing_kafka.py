import json
from datetime import datetime
import random
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
import sqlite3

def produce_kafka_events(max_events=10):
    try:
        print("Iniciando productor Kafka...")
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        print("Productor Kafka iniciado.")

        for _ in range(max_events):
            event = {
                "event_id": f"evt{random.randint(10000, 99999)}",
                "timestamp": datetime.now().isoformat(),
                "user_id": random.randint(1, 1000),
                "event_type": random.choice(["page_view", "click", "purchase"]),
                "url": f"/{random.choice(['home', 'product', 'cart', 'checkout'])}"
            }
            producer.send('user_events', event)
            print(f"Evento producido: {event}")
            time.sleep(1)
    except Exception as e:
        print(f"Error en el productor Kafka: {e}")

def consume_kafka_events(max_events=10):
    try:
        print("Iniciando consumidor Kafka...")
        consumer = KafkaConsumer('user_events',
                                 bootstrap_servers=['localhost:9092'],
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True)
        print("Consumidor Kafka iniciado.")

        conn = sqlite3.connect('fake_google_ads.db')
        c = conn.cursor()

        for _ in range(max_events):
            message = next(consumer)
            event = message.value
            print(f"Mensaje recibido: {event}")
            c.execute("""
                INSERT INTO kafka_events 
                (event_id, timestamp, user_id, event_type, url) 
                VALUES (?, ?, ?, ?, ?)
            """, (
                event['event_id'],
                event['timestamp'],
                event['user_id'],
                event['event_type'],
                event['url']
            ))
            conn.commit()

        conn.close()
    except Exception as e:
        print(f"Error en el consumidor Kafka: {e}")

def start_kafka_processing():
    threading.Thread(target=produce_kafka_events, args=(10,), daemon=True).start()
    threading.Thread(target=consume_kafka_events, args=(10,), daemon=True).start()
    time.sleep(15)  # Esperar a que los hilos terminen

if __name__ == "__main__":
    start_kafka_processing()
import sqlite3
import random
from datetime import datetime, timedelta
import csv
import json
from kafka import KafkaProducer, KafkaConsumer
import time
import threading
# Función para crear la base de datos y llenarla con datos simulados
def create_and_populate_db():
    conn = sqlite3.connect('fake_google_ads.db')
    c = conn.cursor()

    # Crear la tabla
    c.execute('''CREATE TABLE IF NOT EXISTS campaigns
                 (id INTEGER PRIMARY KEY,
                  name TEXT,
                  impressions INTEGER,
                  clicks INTEGER,
                  conversions REAL,
                  cost REAL,
                  date TEXT)''')

    # Generar datos simulados
    campaign_names = ["Summer Sale", "Winter Promotion", "Spring Collection", "Fall Deals", "Holiday Special"]
    start_date = datetime.now() - timedelta(days=30)

    for i in range(1, 101):  # Generar 100 registros
        name = random.choice(campaign_names) + f" {i}"
        impressions = random.randint(1000, 100000)
        clicks = random.randint(10, int(impressions * 0.1))
        conversions = round(random.uniform(0, clicks * 0.1), 2)
        cost = round(random.uniform(10, 1000), 2)
        date = (start_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")

        c.execute("INSERT INTO campaigns (name, impressions, clicks, conversions, cost, date) VALUES (?, ?, ?, ?, ?, ?)",
                  (name, impressions, clicks, conversions, cost, date))

    conn.commit()
    conn.close()

# Función para obtener datos de la base de datos SQLite
def get_google_ads_data(customer_id):
    conn = sqlite3.connect('fake_google_ads.db')
    c = conn.cursor()

    query = """
        SELECT 
            id,
            name,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(conversions) as conversions,
            SUM(cost) as cost
        FROM campaigns
        WHERE date >= date('now', '-30 days')
        GROUP BY id, name
    """
    
    c.execute(query)
    campaigns = []
    for row in c.fetchall():
        campaigns.append({
            "id": row[0],
            "name": row[1],
            "impressions": row[2],
            "clicks": row[3],
            "conversions": row[4],
            "cost": row[5]
        })
    
    conn.close()
    return campaigns

# Función para ingerir datos desde un archivo CSV
def ingest_csv_data(file_path):
    conn = sqlite3.connect('fake_google_ads.db')
    c = conn.cursor()

    with open(file_path, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            c.execute("""
                INSERT OR REPLACE INTO user_interactions 
                (user_id, visit_date, page_views, bounce_rate, session_duration) 
                VALUES (?, ?, ?, ?, ?)
            """, (
                row['user_id'],
                row['visit_date'],
                row['page_views'],
                row['bounce_rate'],
                row['session_duration']
            ))

    conn.commit()
    conn.close()

# Función para simular la producción de eventos en Kafka
def produce_kafka_events():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        event = {
            "event_id": f"evt{random.randint(10000, 99999)}",
            "timestamp": datetime.now().isoformat(),
            "user_id": random.randint(1, 1000),
            "event_type": random.choice(["page_view", "click", "purchase"]),
            "url": f"/{random.choice(['home', 'product', 'cart', 'checkout'])}"
        }
        producer.send('user_events', event)
        time.sleep(1)  # Simular un evento por segundo

# Función para consumir eventos de Kafka y almacenarlos en la base de datos
def consume_kafka_events():
    # Configurar el productor
    producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    conn = sqlite3.connect('fake_google_ads.db')
    c = conn.cursor()

    for message in consumer:
        event = message.value
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

def main():
    try:
        # Crear y poblar la base de datos si no existe
        create_and_populate_db()

        # Crear tablas adicionales para los nuevos datos
        conn = sqlite3.connect('fake_google_ads.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS user_interactions
                     (user_id INTEGER PRIMARY KEY,
                      visit_date TEXT,
                      page_views INTEGER,
                      bounce_rate REAL,
                      session_duration INTEGER)''')
        c.execute('''CREATE TABLE IF NOT EXISTS kafka_events
                     (event_id TEXT PRIMARY KEY,
                      timestamp TEXT,
                      user_id INTEGER,
                      event_type TEXT,
                      url TEXT)''')
        conn.commit()
        conn.close()

        # Ingerir datos desde el archivo CSV
        ingest_csv_data('user_data.csv')

        # Iniciar la producción y consumo de eventos Kafka en hilos separados
        threading.Thread(target=produce_kafka_events, daemon=True).start()
        threading.Thread(target=consume_kafka_events, daemon=True).start()

        # ID de cliente (en este caso, no se usa realmente, pero lo mantenemos por compatibilidad)
        customer_id = "4099398044"
        
        # Usar la función para obtener los datos de campañas
        campaigns = get_google_ads_data(customer_id)
        
        if campaigns:
            print("Campañas obtenidas:")
            for campaign in campaigns:
                print(campaign)
        else:
            print("No se pudieron obtener las campañas.")

        # Mantener el programa en ejecución para que los hilos de Kafka sigan funcionando
        while True:
            time.sleep(1)
    
    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    main()
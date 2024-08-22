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
def get_google_ads_data():
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
def produce_kafka_events(max_events=10):
    try:
        print("Iniciando productor Kafka...")
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        print("Productor Kafka iniciado.")

        for _ in range(max_events):  # Limitar a un número específico de eventos
            event = {
                "event_id": f"evt{random.randint(10000, 99999)}",
                "timestamp": datetime.now().isoformat(),
                "user_id": random.randint(1, 1000),
                "event_type": random.choice(["page_view", "click", "purchase"]),
                "url": f"/{random.choice(['home', 'product', 'cart', 'checkout'])}"
            }
            producer.send('user_events', event)
            print(f"Evento producido: {event}")
            time.sleep(1)  # Simular un evento por segundo
    except Exception as e:
        print(f"Error en el productor Kafka: {e}")

# Función para consumir eventos de Kafka y almacenarlos en la base de datos
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

def analyze_campaign_performance(campaign):
    impressions = campaign.get('impressions', 0)
    clicks = campaign.get('clicks', 0)
    conversions = campaign.get('conversions', 0)
    cost = campaign.get('cost', 0)
    
    ctr = (clicks / impressions * 100) if impressions else 0
    conversion_rate = (conversions / clicks * 100) if clicks else 0

    return f"""Campaña: {campaign.get('name', 'Desconocido')}
  Impresiones: {impressions}
  Clics (del anuncio): {clicks}
  Conversiones (del anuncio): {conversions:.2f}
  Costo: ${cost:.2f}
  CTR (Click-Through Rate): {ctr:.2f}%
  Tasa de conversión (basada en conversiones): {conversion_rate:.2f}%"""



def main():
    try:
        print("Iniciando la aplicación...")

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
        threading.Thread(target=produce_kafka_events, args=(10,), daemon=True).start()
        threading.Thread(target=consume_kafka_events, args=(10,), daemon=True).start()

        # Mantener el programa en ejecución para que los hilos de Kafka sigan funcionando
        time.sleep(15)  # Esperar a que los hilos terminen

        # Obtener y analizar el rendimiento de las campañas
        campaigns = get_google_ads_data()
        for campaign in campaigns:
            print(analyze_campaign_performance(campaign))
    
    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    main()

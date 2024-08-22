import sqlite3
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
import json

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'google_ads_data'

def create_db():
    conn = sqlite3.connect('fake_google_ads.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS campaigns
                 (id INTEGER PRIMARY KEY,
                  name TEXT,
                  impressions INTEGER,
                  clicks INTEGER,
                  conversions REAL,
                  cost REAL,
                  date TEXT,
                  country TEXT,
                  region TEXT,
                  city TEXT,
                  age_group TEXT,
                  gender TEXT,
                  device_type TEXT,
                  campaign_type TEXT,
                  ad_group TEXT,
                  keyword TEXT)''')

    conn.commit()
    conn.close()

def create_and_populate_db(producer):
    campaign_names = ["Summer Sale", "Winter Promotion", "Spring Collection", "Fall Deals", "Holiday Special"]
    countries = ["USA", "UK", "Canada", "Australia", "Germany"]
    regions = ["North", "South", "East", "West", "Central"]
    cities = ["New York", "London", "Toronto", "Sydney", "Berlin"]
    age_groups = ["18-24", "25-34", "35-44", "45-54", "55+"]
    genders = ["Male", "Female", "Other"]
    device_types = ["Mobile", "Desktop", "Tablet"]
    campaign_types = ["Search", "Display", "Video", "Shopping"]
    ad_groups = ["Group A", "Group B", "Group C", "Group D"]
    keywords = ["sale", "discount", "new arrival", "best seller", "limited offer"]

    start_date = datetime.now() - timedelta(days=30)

    for i in range(1, 1001):  # Generar 1000 registros
        name = random.choice(campaign_names) + f" {i}"
        impressions = random.randint(1000, 100000)
        clicks = random.randint(10, int(impressions * 0.1))
        conversions = round(random.uniform(0, clicks * 0.1), 2)
        cost = round(random.uniform(10, 1000), 2)
        date = (start_date + timedelta(days=random.randint(0, 30)))
        country = random.choice(countries)
        region = random.choice(regions)
        city = random.choice(cities)
        age_group = random.choice(age_groups)
        gender = random.choice(genders)
        device_type = random.choice(device_types)
        campaign_type = random.choice(campaign_types)
        ad_group = random.choice(ad_groups)
        keyword = random.choice(keywords)

        data = {
            "name": name,
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "cost": cost,
            "date": date.strftime("%Y-%m-%d"),
            "country": country,
            "region": region,
            "city": city,
            "age_group": age_group,
            "gender": gender,
            "device_type": device_type,
            "campaign_type": campaign_type,
            "ad_group": ad_group,
            "keyword": keyword
        }

        producer.send(KAFKA_TOPIC, json.dumps(data).encode('utf-8'))

def consume_and_insert_to_db(consumer):
    conn = sqlite3.connect('fake_google_ads.db')
    c = conn.cursor()

    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        c.execute("""INSERT INTO campaigns 
                     (name, impressions, clicks, conversions, cost, date, country, region, city, 
                      age_group, gender, device_type, campaign_type, ad_group, keyword) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (data['name'], data['impressions'], data['clicks'], data['conversions'], data['cost'],
                   data['date'], data['country'], data['region'], data['city'], data['age_group'],
                   data['gender'], data['device_type'], data['campaign_type'], data['ad_group'], data['keyword']))

    conn.commit()
    conn.close()

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
            SUM(cost) as cost,
            country,
            region,
            city,
            age_group,
            gender,
            device_type,
            campaign_type,
            ad_group,
            keyword
        FROM campaigns
        WHERE date >= date('now', '-30 days')
        GROUP BY id, name, country, region, city, age_group, gender, device_type, campaign_type, ad_group, keyword
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
            "cost": row[5],
            "country": row[6],
            "region": row[7],
            "city": row[8],
            "age_group": row[9],
            "gender": row[10],
            "device_type": row[11],
            "campaign_type": row[12],
            "ad_group": row[13],
            "keyword": row[14]
        })
    
    conn.close()
    return campaigns

def main():
    try:
        # Crear la base de datos si no existe
        create_db()
        
        # Configurar el productor de Kafka
        producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS])
        
        # Generar y enviar datos a Kafka
        create_and_populate_db(producer)
        
        # Configurar el consumidor de Kafka
        consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], auto_offset_reset='earliest')
        
        # Consumir mensajes de Kafka e insertar en la base de datos
        consume_and_insert_to_db(consumer)
        
        # ID de cliente (en este caso, no se usa realmente, pero lo mantenemos por compatibilidad)
        customer_id = "4099398044"
        
        # Usar la función para obtener los datos
        campaigns = get_google_ads_data(customer_id)
        
        if campaigns:
            print(f"Se obtuvieron {len(campaigns)} campañas:")
            for campaign in campaigns[:5]:  # Mostrar solo las primeras 5 campañas
                print(campaign)
            print("...")
        else:
            print("No se pudieron obtener las campañas.")
    
    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    main()
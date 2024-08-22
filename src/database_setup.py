import psycopg2
import random
from datetime import datetime, timedelta
import csv
import os

# Configuración de Redshift
redshift_host = "default-workgroup.637423291933.us-west-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_dbname = "dev"
redshift_user = os.environ.get("REDSHIFT_USER", "admin")
redshift_password = os.environ.get("REDSHIFT_PASSWORD", "Fany1711")

def connect_to_redshift():
    return psycopg2.connect(
        host=redshift_host,
        port=redshift_port,
        dbname=redshift_dbname,
        user=redshift_user,
        password=redshift_password
    )

def create_tables():
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            # Crear las tablas
            cur.execute('''CREATE TABLE IF NOT EXISTS campaigns
                        (id BIGINT IDENTITY(1,1) PRIMARY KEY,
                        name VARCHAR(255),
                        impressions INTEGER,
                        clicks INTEGER,
                        conversions REAL,
                        cost REAL,
                        date DATE,
                        country VARCHAR(50),
                        region VARCHAR(50),
                        city VARCHAR(50),
                        age_group VARCHAR(20),
                        gender VARCHAR(10),
                        device_type VARCHAR(20),
                        campaign_type VARCHAR(20),
                        ad_group VARCHAR(50),
                        keyword VARCHAR(50))''')

            cur.execute('''CREATE TABLE IF NOT EXISTS user_interactions
                        (user_id INTEGER PRIMARY KEY,
                        visit_date DATE,
                        page_views INTEGER,
                        bounce_rate REAL,
                        session_duration INTEGER)''')

            cur.execute('''CREATE TABLE IF NOT EXISTS kafka_events
                        (event_id VARCHAR(255) PRIMARY KEY,
                        timestamp TIMESTAMP,
                        user_id INTEGER,
                        event_type VARCHAR(50),
                        url VARCHAR(255))''')

def populate_campaigns():
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
    
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            for i in range(1, 1001):  # Generar 1000 registros
                name = f"{random.choice(campaign_names)} {i}"
                impressions = random.randint(1000, 100000)
                clicks = random.randint(10, int(impressions * 0.1))
                conversions = round(random.uniform(0, clicks * 0.1), 2)
                cost = round(random.uniform(10, 1000), 2)
                date = (start_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
                country = random.choice(countries)
                region = random.choice(regions)
                city = random.choice(cities)
                age_group = random.choice(age_groups)
                gender = random.choice(genders)
                device_type = random.choice(device_types)
                campaign_type = random.choice(campaign_types)
                ad_group = random.choice(ad_groups)
                keyword = random.choice(keywords)

                cur.execute("""
                    INSERT INTO campaigns 
                    (name, impressions, clicks, conversions, cost, date, country, region, city, 
                    age_group, gender, device_type, campaign_type, ad_group, keyword) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (name, impressions, clicks, conversions, cost, date, country, region, city,
                    age_group, gender, device_type, campaign_type, ad_group, keyword))

def ingest_csv_data(file_path):
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            with open(file_path, 'r') as csvfile:
                csvreader = csv.DictReader(csvfile)
                for row in csvreader:
                    cur.execute("""
                        DELETE FROM user_interactions WHERE user_id = %s
                    """, (row['user_id'],))

                    cur.execute("""
                        INSERT INTO user_interactions 
                        (user_id, visit_date, page_views, bounce_rate, session_duration) 
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        row['user_id'],
                        row['visit_date'],
                        row['page_views'],
                        row['bounce_rate'],
                        row['session_duration']
                    ))

if __name__ == "__main__":
    create_tables()
    populate_campaigns()
    ingest_csv_data('user_data.csv')

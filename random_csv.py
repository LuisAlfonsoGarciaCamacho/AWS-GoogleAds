import csv
import random
from datetime import datetime, timedelta

# Función para generar datos falsos y guardarlos en un archivo CSV
def generate_fake_csv_data(file_path, num_records):
    with open(file_path, 'w', newline='') as csvfile:
        fieldnames = ['user_id', 'visit_date', 'page_views', 'bounce_rate', 'session_duration']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        
        start_date = datetime.strptime('2024-08-01', '%Y-%m-%d')
        
        for i in range(1, num_records + 1):
            visit_date = (start_date + timedelta(days=random.randint(0, 14))).strftime('%Y-%m-%d')
            page_views = random.randint(1, 10)
            bounce_rate = round(random.uniform(0.1, 0.5), 2)
            session_duration = random.randint(60, 600)
            
            writer.writerow({
                'user_id': i,
                'visit_date': visit_date,
                'page_views': page_views,
                'bounce_rate': bounce_rate,
                'session_duration': session_duration
            })

# Llamada a la función para generar 100 registros en 'user_data.csv'
generate_fake_csv_data('user_data.csv', 100)

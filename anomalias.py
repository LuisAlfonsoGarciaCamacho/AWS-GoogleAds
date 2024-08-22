import pandas as pd
import sqlite3

# Conectar a la base de datos SQLite
conn = sqlite3.connect('fake_google_ads.db')
query = "SELECT * FROM campaigns"
df = pd.read_sql_query(query, conn)
conn.close()

# Eliminar duplicados
df.drop_duplicates(inplace=True)

# Rellenar o eliminar valores nulos según sea necesario
df.fillna(0, inplace=True)

# Calcular Costo por Clic (CPC) y Tasa de Conversión
df['CPC'] = df['cost'] / df['clicks']
df['conversion_rate'] = df['conversions'] / df['clicks']

# Lidiar con divisiones por cero
df['CPC'] = df['CPC'].replace([float('inf'), -float('inf')], 0)
df['conversion_rate'] = df['conversion_rate'].replace([float('inf'), -float('inf')], 0)

# Detectar anomalías en las métricas calculadas
anomalous_data = df[(df['CPC'] > df['CPC'].mean() * 3) | (df['conversion_rate'] > 1)]
if not anomalous_data.empty:
    print("Anomalías detectadas en los datos:")
    print(anomalous_data)

# Crear la base de datos si no existe y almacenar los datos transformados
conn = sqlite3.connect('analytics.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY,
                campaign_id INTEGER,
                date TEXT,
                impressions INTEGER,
                clicks INTEGER,
                conversions REAL,
                cost REAL,
                CPC REAL,
                conversion_rate REAL
            )''')

conn.commit()

# Almacenar los datos transformados
df.to_sql('analytics', conn, if_exists='replace', index=False)
conn.close()

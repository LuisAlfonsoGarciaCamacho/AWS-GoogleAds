import sqlite3
import pandas as pd

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
conn.close()

conn = sqlite3.connect('analytics.db')
df.to_sql('analytics', conn, if_exists='replace', index=False)
conn.close()

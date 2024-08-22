import os
import psycopg2

# Configurar las variables de entorno
def set_redshift_env_vars():
    os.environ["REDSHIFT_USER"] = "admin"
    os.environ["REDSHIFT_PASSWORD"] = "Fany1711"

# Llamar a la función para configurar las variables de entorno
set_redshift_env_vars()

# Extraer el host, puerto y nombre de la base de datos del endpoint
redshift_url = "default-workgroup.637423291933.us-west-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_dbname = "dev"
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")

# Conectarse a Redshift
conn = None
cursor = None
try:
    conn = psycopg2.connect(
        dbname=redshift_dbname,
        user=redshift_user,
        password=redshift_password,
        host=redshift_url,
        port=redshift_port
    )
    cursor = conn.cursor()

    # Crear una tabla de prueba
    create_table_query = """
    CREATE TABLE test_table (
        id INT PRIMARY KEY,
        name VARCHAR(50),
        age INT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Tabla 'test_table' creada exitosamente.")

except Exception as e:
    print("Ocurrió un error al conectarse a Redshift o al crear la tabla:", e)

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

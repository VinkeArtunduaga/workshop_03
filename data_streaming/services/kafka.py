from kafka import KafkaProducer, KafkaConsumer
import json
import pandas as pd
import time
import psycopg2
from sklearn.linear_model import LinearRegression
import joblib

# Carga de la configuración de la base de datos desde un archivo JSON
with open("C:/Users/kevin/ETL/workshop_03/data_streaming/services/df_config.json") as config_file:
    db_config = json.load(config_file)

def create_table(db_config):
    try:

        conn = psycopg2.connect(
            host='localhost',
            user=db_config['user'],
            password=db_config['password'],
            database='happiness'
        )

        cursor = conn.cursor()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS happiness_predictions (
            id SERIAL PRIMARY KEY,
            economy FLOAT,
            social_support FLOAT,
            health FLOAT,
            happiness_test FLOAT,
            happiness_prediction FLOAT
        );
        """
        cursor.execute(create_table_sql)

        conn.commit()
        conn.close()

        print("Tabla 'happiness_predictions' creada con éxito.")
    except Exception as e:
        print(f"Error al crear la tabla 'happiness_predictions': {e}")


def kafka_producer():
    df = pd.read_csv("C:/Users/kevin/ETL/workshop_03/testing_data.csv")
    producer = KafkaProducer(
        value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092']
    )

    for index, row in df.iterrows():
        data = {
            "economy": row["economy"],
            "social_support": row["social_support"],
            "health": row["health"],
            "happiness_test": row["happiness_score"]
        }
        producer.send("workshop_03", value=data)
        print("Mensaje enviado")
        time.sleep(0.01)


loaded_model = joblib.load("C:/Users/kevin/ETL/workshop_03/modelo_entrenado.pkl")


def process_and_store_message(message, loaded_model, db_config):

    try:

        # Extrae las características del mensaje
        economy = message["economy"]
        social_support = message["social_support"]
        health = message["health"]
        happiness_score = message["happiness_test"]

        prediction = loaded_model.predict([[economy, social_support, health]])

        conn = psycopg2.connect(
            host='localhost',
            user=db_config['user'],
            password=db_config['password'],
            database='happiness'
        )

        cursor = conn.cursor()

        insert_data_sql = """
        INSERT INTO happiness_predictions (economy, social_support, health, happiness_test, happiness_prediction)
        VALUES (%s, %s, %s, %s, %s);
        """
        cursor.execute(insert_data_sql, (economy, social_support, health, happiness_score, prediction[0]))

        conn.commit()
        conn.close()

        print("Datos almacenados con éxito en la base de datos.")

    except Exception as e:
        print(f"Error al procesar y almacenar el mensaje: {e}")


def kafka_consumer():
    consumer = KafkaConsumer(
        'workshop_03',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
    )

    for message in consumer:
        process_and_store_message(message.value, loaded_model, db_config)

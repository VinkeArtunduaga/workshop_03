from services.kafka import kafka_producer, kafka_consumer, create_table
import json

with open("C:/Users/kevin/ETL/workshop_03/data_streaming/services/df_config.json") as config_file:
        db_config = json.load(config_file)

if __name__ == "__main__":
    create_table(db_config)
    kafka_producer()
    kafka_consumer()
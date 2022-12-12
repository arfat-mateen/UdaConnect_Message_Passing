import json
import os

import psycopg2
from kafka import KafkaConsumer

DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]

KAFKA_TOPIC = "locations"
KAFKA_PATH = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'

consumer = KafkaConsumer(bootstrap_servers=KAFKA_PATH)

def ingest_location(location):
    connection = psycopg2.connect(
        dbname = DB_NAME,
        user = DB_USERNAME,
        password = DB_PASSWORD,
        host = DB_HOST,
        port = DB_PORT,
    )

    query = "INSERT INTO location (person_id, coordinate) VALUES ({}, ST_Point({}, {}));".format(
        int(location["person_id"]),
        location["latitude"],
        location["longitude"],
    )

    with connection.cursor() as cursor:
        try:
            query = "INSERT INTO location (person_id, coordinate) VALUES ({}, ST_Point({}, {}));".format(
                int(location["person_id"]),
                location["latitude"],
                location["longitude"],
            )

            cursor.execute(query)
            print(f"Location added with person id: {location['person_id']}")
        except Exception as error:
            print(f"ERROR: {error}")


def consume():
    for message in consumer:
        print(f"Location: {message}")

        decoded_message = message.value.decode("utf-8")
        location = json.loads(decoded_message)

        ingest_location(location)


if __name__ == "__main__":
    consume()
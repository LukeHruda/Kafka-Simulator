from kafka import KafkaConsumer
from kafka.vendor.six import assertRaisesRegex
import psycopg2
from pgcopy import CopyManager
from threading import Thread
from time import sleep


def threaded_function(index):
    consumer = arr[index]
    while 1:
        for message in consumer:
            print(message.topic,end ="")
            data = message.value
            data = data.decode("ascii")
            print(" ",end ="")
            print(data)
            # Create query to simulate data. Modify the interval properties to change the ingest rate into
            # the database... from tens of rows to hundreds of thousands of rows
            query_simulation_data = f"""INSERT INTO sensordata VALUES (now(),'{index+1}','{data}');"""
            # Run the above created query and retrieve all simulation data from it
            cursor.execute(query_simulation_data)
            conn.commit()


arr = []

if __name__ == "__main__":
    consumer = KafkaConsumer()

    topics = consumer.topics()



    # Create the native queries to create the tables which will be storing data #
    # Query for creating relational table
    query_create_sensors_table = "CREATE TABLE sensors (id serial PRIMARY KEY NOT NULL, type VARCHAR(108), groupId INTEGER);"
    # Query for creating actual sensor data table so we can create a hypertable (Timescale specific)
    query_create_sensor_data_table = """CREATE TABLE sensordata (
        time TIMESTAMPTZ NOT NULL,
        sensor_id INTEGER,
        data_value DOUBLE PRECISION,
        FOREIGN KEY (sensor_id) REFERENCES sensors (id));"""
    # Query to create hypertable based on sensordata table
    query_create_sensor_data_hypertable = "SELECT create_hypertable('sensordata', 'time')"
    #
    # Create connection with postgres instance. Syntax: "postgres://${NEW_ROLE_NAME}:${NEW_ROLE_PASSWORD}@host:port/dbname"
    CONNECTION = "postgres://luke:testing@localhost:5432/postgres"
    conn = psycopg2.connect(CONNECTION)
    # Create the object to manage our queries
    cursor = conn.cursor()
    #
    # Remove all tables created in previous test...
    cursor.execute("DROP TABLE IF EXISTS sensordata")
    cursor.execute("DROP TABLE IF EXISTS sensors")
    # Commit those changes in the database
    conn.commit()
    # Create relational table, data table, and hypertable...
    cursor.execute(query_create_sensors_table)
    cursor.execute(query_create_sensor_data_table)
    cursor.execute(query_create_sensor_data_hypertable)

    conn.commit()

    for topic in topics:
        print(topic)
        consumer = KafkaConsumer(topic, consumer_timeout_ms=100)
        for message in consumer:
            if message.value is not None:
                print("Topic is Online!")
                arr.append(consumer)
                break

    for index, consumer in enumerate(arr):
        for message in consumer:
            # Create query to add each unique sensor into the relational table as they are introduced
            query_simulation_sensor_creation = f"""INSERT INTO sensors (id, type, groupId) 
                                                VALUES ('{index+1}', '{message.topic}', 0) ON CONFLICT DO NOTHING; """
            cursor.execute(query_simulation_sensor_creation)
            thread = Thread(target = threaded_function, args = (index,))
            thread.start()
            break


   
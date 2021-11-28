"""
    Configuration Handler
"""

import sys, os
import json
from Parameters import Parameters
Parameters = Parameters.instance()

import time 
import random
import threading
import psycopg2
from json import dumps
from pgcopy import CopyManager
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id='test'
)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
consumer = KafkaConsumer()


topics = consumer.topics()

class Configuration :

    """
        Wrapper class to import JSON configuration files
    """

    class Simulator(threading.Thread) : 

        def __init__(self, parameters, rate) : 
            threading.Thread.__init__(self)
            self.parameters = []
            #
            for attr_name in parameters : 
                self.parameters.append(getattr(Parameters, attr_name))
            #
            self.rate = rate
            #
            self.start()

        def run(self) :
            # Create the native queries to create the tables which will be storing data #
            # Query for creating relational table
         #   query_create_sensors_table = "CREATE TABLE sensors (id serial PRIMARY KEY NOT NULL, type VARCHAR(108), groupId INTEGER);"
            # Query for creating actual sensor data table so we can create a hypertable (Timescale specific)
         #   query_create_sensor_data_table = """CREATE TABLE sensordata (
         #       time TIMESTAMPTZ NOT NULL,
         #       sensor_id INTEGER,
         #       data_value DOUBLE PRECISION,
         #       FOREIGN KEY (sensor_id) REFERENCES sensors (id));"""
            # Query to create hypertable based on sensordata table
         #   query_create_sensor_data_hypertable = "SELECT create_hypertable('sensordata', 'time')"
            #
            # Create connection with postgres instance. Syntax: "postgres://${NEW_ROLE_NAME}:${NEW_ROLE_PASSWORD}@host:port/dbname"
         #   CONNECTION = "postgres://luke:testing@localhost:5432/luke"
         #   conn = psycopg2.connect(CONNECTION)
            # Create the object to manage our queries
         #   cursor = conn.cursor()
            #
            # Remove all tables created in previous test...
        #    cursor.execute("DROP TABLE IF EXISTS sensordata")
        #    cursor.execute("DROP TABLE IF EXISTS sensors")
            # Commit those changes in the database
        #    conn.commit()
            # Create relational table, data table, and hypertable...
        #    cursor.execute(query_create_sensors_table)
        #    cursor.execute(query_create_sensor_data_table)
        #    cursor.execute(query_create_sensor_data_hypertable)

        #    conn.commit()
            arr = []
            for index, p in enumerate(self.parameters):
                arr.append(random.randrange(0, 100) / random.randint(1, 10))

            while True:
                time.sleep(self.rate)
                for index, p in enumerate(self.parameters):
                    x = arr[index]
                    x =  x + random.randrange(-1, 1)/ random.randint(1, 10)
                    p.setValue(x)
                    data = {'number' : x}
                    result = producer.send(p.name, value=x)

                    # Create query to add each unique sensor into the relational table as they are introduced
         #           query_simulation_sensor_creation = f"""INSERT INTO sensors (id, type, groupId) 
           #                                             VALUES (DEFAULT, '{p.name}', 0) ON CONFLICT DO NOTHING; """
          #          cursor.execute(query_simulation_sensor_creation)
                    # Create query to simulate data. Modify the interval properties to change the ingest rate into
                    # the database... from tens of rows to hundreds of thousands of rows
           #         query_simulation_data = f"""SELECT generate_series(now() - interval '24 hour', now(), interval '5 
           #         minute') AS time, {index+1} as sensor_id, {str(x)} as data_value;"""
           #         # Run the above created query and retrieve all simulation data from it
           #         cursor.execute(query_simulation_data)
           #         values = cursor.fetchall()
            #        # Setup the copy manager to write data to our instance considerably faster
            ##        cols = ['time', 'sensor_id', 'data_value']
            #        copyMgr = CopyManager(conn, 'sensordata', cols)
            #        copyMgr.copy(values)
                    # Commit all changes made in this loop (for this sensor) to the database tables (relational and
                    # hypertable in this case)
             #       conn.commit()
            # Close the cursor once done
            #cursor.close()

    class Parameter :
        """
            Parameter class to wrap individual items into parameters 
        """
        def __init__(self, name, default_value=None, description=None, unit=None) :
            print('PARAM')
            self.name = name 
            self.value = default_value
            self.description = description
            self.unit = unit 
            ### 
            self.subscribers = []
            print(name, " added!")
        """ 
            Using set value method will invoke event
        """ 

        def toList(self) : 
            return [self.name, self.value, self.unit, self.description]

        def setValue(self, value) :
            self.value = value 
            self.publish()

        def subscribe(self, callback_function) :
            self.subscribers.append(callback_function)

        def publish(self) :
            if len(self.subscribers) > 0 :
                for subscriber in self.subscribers : 
                    try :
                        subscriber(self.value)
                    except Exception as e :
                        self.subscribers.remove(subscriber)
    
    def __init__(self, configuration_file=None, simulator=False, sim_rate=0.1) :
        self.file = configuration_file
        self.loaded = False
        self.keys = []
        self.load()
        if simulator == True and self.loaded == True :
            self.Simulator(self.keys, sim_rate)
    
    def load(self) :
        print('Loading...')
        self.keys = []
        try : 
            data = json.load(open(self.file)) 
        except Exception as e :
            print(e)
            raise Exception("An error occured loading the configuration.\n"+e)
        else :  
            for par,cfg in data.items() :
                ## eventually we should make this pass a tupple
                self.keys.append(par)
                param = self.Parameter(par, default_value=cfg["value"], description=cfg["description"], unit=cfg["unit"])
                print(param)
                try : 
                    Parameters.addParameter(param)
                except ValueError as ve :
                    print(ve)
            #
            self.loaded = True

    def unLoad(self) : 
        try :
            Parameters.removeParameters(self.keys) 
        except Exception as e :
            print(e)
        
    
    
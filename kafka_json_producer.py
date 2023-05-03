#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "/Users/shashankmishra/Desktop/Batch-1/Kafka Classes/Confluen Kafka Setup/Confluent-Kafka-Setup/cardekho_dataset.csv"
columns=['car_name', 'brand', 'model', 'vehicle_age', 'km_driven', 'seller_type',
       'fuel_type', 'transmission_type', 'mileage', 'engine', 'max_power',
       'seats', 'selling_price']

API_KEY = 'HCBRVB54E2TFDNVO'
ENDPOINT_SCHEMA_URL  = 'https://psrc-epkz2.ap-southeast-2.aws.confluent.cloud'
API_SECRET_KEY = 'oCEA0rTyVDrw4eMDq6BXNx1AbdHt+4AtCu/Pz2DbUaO10R1mrofmRsnJdfG7qVdd'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'JU22OSOMINSRFVTT'
SCHEMA_REGISTRY_API_SECRET = 'D0456tLryJO8tbukigXFJJA2r9Xrd6Trvch85cYWMsmkAIOfemGxWa69X5FJCshJ'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Car:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record # This create an instance variable with the name record that holds the dictionary as it is.
   
    @staticmethod
    def dict_to_car(data:dict,ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"
'''
This line takes dictionary as a input and set key of dictionary as class attributes and values of dictionary as value. It is used by below funtion get_car_instance.

'''

def get_car_instance(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,1:]
    cars:List[Car]=[]
    for data in df.values:
        car=Car(dict(zip(columns,data)))
        cars.append(car)
        yield car
              
'''
Above function takes file path as an input and read it as a data frame. Then removes its first value and read it line by line i.e. 1 row at a time and zip it with 
column names and converts it into a car object, every time this function yield a car object(i.e. returns current line by converting to car object) and at the same 
time it is created a list of cars and appends each object one by one as a whole.
'''

def car_to_dict(car:Car, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return car.record
# Simply returns record instance variable which is holding dictionary as it is 

def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    schema_registry_conf = schema_config() # Calls schema_config that returns dictionary of schema registry attributes needed for authentications and more.
    schema_registry_client = SchemaRegistryClient(schema_registry_conf) # Basically passing schema registry attributes to instance of this class.
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value' # Creating a subject variable according to convention <topic_name-value> here value is verion of schema registry.

    schema = schema_registry_client.get_latest_version(subject)
'''
Above line uses instance schema_registry_client to access it's function get_latest_version which returns following
        return RegisteredSchema(schema_id=response['id'],
                                schema=Schema(response['schema'],
                                              schema_type,
                                              response.get('references', [])),
                                subject=response['subject'],
                                version=response['version'])
that means schema variable used above is nothing but an instance of class RisteredSchema(). here schema is a instance variable of class RegisteredSchema which is 
again creating an instance of Schema classs.
'''
    schema_str=schema.schema.schema_str
'''
first schema is instance of RegisteredSchema class used above again second schema is it's instance variable which is again a instance of Schema class and in Schema class there is a varibale
named as schema_str, we are fetching that schema_str value and assigning it to a variable called as schem_str.
'''

    string_serializer = StringSerializer('utf_8') # used to generate key for producer class.
    json_serializer = JSONSerializer(schema_str, schema_registry_client, car_to_dict) # Above fetched schema_str we are using here to create an object of this class.


    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0) 
    try:
        for car in get_car_instance(file_path=FILE_PATH): # get_car_instance yields car object of type Car line by line
            print(car)
            producer.produce(topic=topic, # Passing name of the topic
                            key=string_serializer(str(uuid4())), # This line is crating key to be binded with value which will be then need for hashing and partition
                            value=json_serializer(car, SerializationContext(topic, MessageField.VALUE)), # Creating values using json_serializer
                            on_delivery=delivery_report)
            break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("car_topic")

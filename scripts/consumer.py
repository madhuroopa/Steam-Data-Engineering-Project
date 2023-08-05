import pandas as pd
import re
import time
import datetime
from bs4 import BeautifulSoup
import requests
from kafka import KafkaConsumer,KafkaProducer
import json
from json import dumps,loads

class Consumer:
    def __init__(self):
        self.topic='weekly-top-sellers'
    def get_consumer(self):
        print("Starting consumer...")
        consumer = KafkaConsumer(self.topic, bootstrap_servers=['Mittu:9092'])
        received_messages = []
        try:
            for message in consumer:
                json_data = message.value.decode('utf-8')  # Convert bytes to string
                print(json_data)
        except Exception as e:
            print("Error in consumer loop:")
        finally:
            consumer.close()

if __name__=="__main__":
    obj=Consumer()
    obj.get_consumer()
    
    
import pandas as pd
import re
import time
import datetime
from bs4 import BeautifulSoup
import requests
from kafka import KafkaConsumer,KafkaProducer
import json
from json import dumps,loads
from kafka.admin import KafkaAdminClient, NewTopic
import os
from datetime import date
class Monthly_data_consumer:
    def __init__(self):
        self.collection_date = date.today()

        self.topic='monthly-visitor-data'
        self.consumer = KafkaConsumer(self.topic, bootstrap_servers=['192.168.0.108:9092'],
                                       group_id='monthly_data_streaming_group',
                                        auto_offset_reset='earliest',   
                                         enable_auto_commit=True,
                                        auto_commit_interval_ms=5000)
    def get_monthly_visitor_data(self):
     
        for message in self.consumer:
                data = json.loads(message.value.decode('utf-8'))
                print(data)
                with open(f'../data/monthly_data/{self.collection_date}_monthy_visitor_data', 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                print(f"Data saved as JSON: monthly_visitor_data_{message.timestamp}.json")
                self.consumer.close()
if __name__ == '__main__':
    obj = Monthly_data_consumer()
    obj.get_monthly_visitor_data()
               
                    
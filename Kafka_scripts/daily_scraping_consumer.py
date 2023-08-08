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
class Daily_data_consumer:
    def __init__(self):
        self.collection_date = date.today()

        self.topic='daily-data'
        self.consumer = KafkaConsumer(self.topic, bootstrap_servers=['192.168.0.108:9092'],
                                       group_id='daily_data_streaming_group',
                                        auto_offset_reset='earliest',   
                                         enable_auto_commit=True,
                                        auto_commit_interval_ms=5000)
    def get_daily_data(self):
     
        for message in self.consumer:
                    data = message.value.decode('utf-8')
                    print(data)
                    df = pd.read_json(data, orient='records')
                    df.to_csv(f'../data/daily_data/{self.collection_date}_most_played.csv', index=False)
                    self.consumer.close()
if __name__ == '__main__':
    obj = Daily_data_consumer()
    obj.get_daily_data()
               
                    
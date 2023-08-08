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
class Weekly_data_Consumer:
    def __init__(self):
        self.collection_date = date.today()
        self.topic='weekly-top-sellers'
        self.reviews_topic="weekly-reviews"
        self.news_topic="weekly-news"
       
       
       
        self.consumer = KafkaConsumer(self.topic,self.reviews_topic,self.news_topic, bootstrap_servers=['192.168.0.108:9092'],
                                       group_id='weekly_data_streaming_group',
                                        auto_offset_reset='earliest',   
                                         enable_auto_commit=True,
                                        auto_commit_interval_ms=5000  )
    
    def get_weekly_top_sellers_consumer(self):
        print("Starting consumer...")
        i=0
        try:
            for message in self.consumer:
            
                topic = message.topic
                data = message.value.decode('utf-8')
                if topic == self.topic:
                    print(data)
                    df = pd.read_json(data, orient='records')
                    df.to_csv(f'../data/weekly_data/top_sellers/{self.collection_date}_weekly_top_sellers.csv', index=False)
                if topic == self.reviews_topic:
                    reviews_list=[]
                    for row in eval(data):
                        reviews_list.append(row)
                    reviews_df=pd.DataFrame(reviews_list,columns=['App Id','Review','Voted Up'])
                    reviews_df.to_csv(f'../data/weekly_data/reviews/{self.collection_date}_weekly_top_sellers_reviews.csv', index=False)
                if topic == self.news_topic:
                    
                    with open (f'../data/weekly_data/news/{self.collection_date}_news_app_{i}',"w") as json_file:
                       json.dump(json.loads(data), json_file, indent=4)
                       i += 1  
        except KeyboardInterrupt:
                print('consumer interrupted')
        finally:
                self.consumer.close()
                       
if __name__=="__main__":
    obj=Weekly_data_Consumer()
    obj.get_weekly_top_sellers_consumer()
   
    #obj.get_weekly_top_sellers_consumer()
    #obj2.get_app_reviews()
    #bootstrap_servers = "Mittu:9092"
    #admin_client = KafkaAdminClient(bootstrap_servers=['Mittu:9092'])
    #topics = admin_client.list_topics()
    #topic_names=[]
    #for topic in topics:
     #   if topic.startswith("game_reviews"):
      #      topic_names.append(topic)
    #consumer_instance = Consumer(bootstrap_servers, topic_names)
    #consumer_instance.get_app_reviews()
    
    
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
class Consumer:
    def __init__(self):
        self.topic='weekly-top-sellers'
        
        self.base_directory="../data/weekly_data/reviews/"
    def save_review_to_json(self, topic_name, review_data):
        # Create a directory for topic if it doesn't exist
        topic_directory = os.path.join(self.base_directory, topic_name)
        os.makedirs(topic_directory, exist_ok=True)

        # Save the review data as a JSON file
        file_path = os.path.join(topic_name, f"review_{len(os.listdir(topic_name)) + 1}.json")
        with open(file_path, 'w') as json_file:
            json.dump(review_data, json_file, indent=4)
            
    def get_weekly_top_sellers_consumer(self):
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
    def get_app_reviews(self):
        admin_client = KafkaAdminClient(bootstrap_servers=['Mittu:9092'])
        topic_names = [topic for topic in admin_client.list_topics() ]
        print(topic_names)
        consumers=[]
        for topic in topic_names[:2]:
            print(topic)
            review_consumer = KafkaConsumer(self.topic,bootstrap_servers=['Mittu:9092'])
            consumers.append(review_consumer)
        while True:
            try:
                for con,topic_name in zip(consumers,topic_names):
                    for message in con:
                        print(message.value.decode('utf-8'))
                        #review_data = json.loads(message.value)  # Assuming message is JSON
                        #print(f"Received review for topic {topic_name}: {review_data}")
                        #self.save_review_to_json(topic_name, review_data)
            except KeyboardInterrupt:
                for con in consumers:
                    con.close()
                        
            
if __name__=="__main__":
    obj=Consumer()
   # obj.get_weekly_top_sellers_consumer()
    obj.get_app_reviews()
    
    
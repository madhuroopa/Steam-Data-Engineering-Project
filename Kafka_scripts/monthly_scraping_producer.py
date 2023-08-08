#Scrapes the monthly website visitor count, engagements
from selenium import webdriver
import time
import json
from datetime import date
from bs4 import BeautifulSoup
from kafka import KafkaConsumer,KafkaProducer
import json
from json import dumps,loads
from kafka.admin import KafkaAdminClient, NewTopic

class MonthlyScraping:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.wd = webdriver.Chrome(options=self.options)
        self.today = date.today()
        self.monthly_visits_url = 'https://data.similarweb.com/api/v1/data?domain=store.steampowered.com'
        self.news_url = "http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid="
        self.topic="monthly-visitor-data"
        self.producer=None
    def kafka_producer(self):
        producer = None
        try:
            producer = KafkaProducer(bootstrap_servers=['192.168.0.108:9092'])
            print("created producer object")
        except Exception as x:
            print("Exception connection to kafka server")
            print(x)
        finally:
            return producer
    def publish_message(self,producer,topic,records):
        try: 
            producer.send(topic, records.encode('utf-8'))
                    
            producer.flush()
            print(f'Message published successfully from producer{topic}.')
        except Exception as e:
            print(f"Error publiching the message error: {e}")

    def get_monthly_visits(self):
        self.wd.get(self.monthly_visits_url)
        self.wd.implicitly_wait(10)
        data = self.wd.page_source
        soup = BeautifulSoup(data, 'html.parser')
        pre_tag = soup.find('pre')
        json_data = pre_tag.text if pre_tag else None
        self.producer=self.kafka_producer()
        
        if json_data:
            self.publish_message(self.producer,self.topic,json.dumps(json.loads(json_data)))
            self.producer.flush()
            print(f"data sent to kafka topic{self.topic}")
        else:
            print("Failed to retrieve valid JSON data. Check the URL and API response.")  
            
      
        self.producer.flush()
        self.producer.close()
       

if __name__ == '__main__':
    monthly_scraping = MonthlyScraping()
    monthly_scraping.get_monthly_visits()            
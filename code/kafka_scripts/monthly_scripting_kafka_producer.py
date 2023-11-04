from kafka import KafkaProducer
import json
from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import date

class MonthlyScraping:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        #self.options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.1234.56 Safari/537.36')
        self.wd = webdriver.Chrome(options=self.options)
        self.today = date.today()
        self.monthly_visits_url = 'https://data.similarweb.com/api/v1/data?domain=store.steampowered.com'
        self.news_url = "http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid="
        self.kafka_broker = '54.242.137.34:9092'
        self.kafka_topic = 'monthly_visitor_data_v1'
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker, 
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def get_monthly_visits(self):
        self.wd.get(self.monthly_visits_url)
        self.wd.implicitly_wait(10)
        data = self.wd.page_source
        print(data)
        soup = BeautifulSoup(data, 'html.parser')
        pre_tag = soup.find('pre')
	
        json_data = pre_tag.text if pre_tag else None

        if json_data:
            self.producer.send(self.kafka_topic, value=json.loads(json_data))
            self.producer.flush()
            print("Data sent to Kafka topic successfully.")
        else:
            print("Failed to retrieve valid JSON data. Check the URL and API response.")

        self.producer.send(self.kafka_topic, value="END_OF_STREAM")
        self.producer.flush()
        self.producer.close()

if __name__ == '__main__':
    monthly_scraping = MonthlyScraping()
    monthly_scraping.get_monthly_visits()

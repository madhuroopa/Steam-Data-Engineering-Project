from selenium import webdriver
from selenium.webdriver.common.by import By
from WebDriverCreation import WebDriverCreation
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
class WeeklyTopSellers:

    def __init__(self):
        self.all_game_rows = None
        self.games = []
        self.games_appid = []
        self.topic='weekly-top-sellers'
        self.review_topic="weekly-reviews"
        self.news_topic="weekly-news"
        self.collection_data = None
        self.driver_instance=WebDriverCreation()
        self.wd=self.driver_instance.wd
        self.base_url = "https://store.steampowered.com/charts/topsellers/US"
        self.news_url = "http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid="
        self.reviews_url = 'https://store.steampowered.com/appreviews/'
        self.positive_reviews=[]
        self.negative_reviews=[]
        self.reviews=[]
        self.news_data=None
        self.producer=None
        
        self.url = self._construct_url_with_last_to_last_tuesday_date()

    def _get_last_to_last_tuesday(self):
        today = datetime.date.today()
        days_since_last_tuesday = (today.weekday() - 1) % 7
        last_tuesday = today - datetime.timedelta(days=days_since_last_tuesday)
        last_to_last_tuesday = last_tuesday - datetime.timedelta(weeks=1)
        return last_to_last_tuesday.strftime("%Y-%m-%d")

    def _construct_url_with_last_to_last_tuesday_date(self):
        self.collection_data = self._get_last_to_last_tuesday()
        print(f"{self.base_url}/{self.collection_data}")
        return f"{self.base_url}/{self.collection_data}"



    def get_data(self):
        #self.scroll_page(self.wd)
        self.wd.get(self.url)
        time.sleep(3)
        button = self.wd.find_element(By.CLASS_NAME, 'DialogButton._DialogLayout.Primary.Focusable')
        button.click()
        #self.wd.get(self.url)

    def expand_all(self):
        time.sleep(5)
        self.wd.get(self.url)

    def get_games(self):
        time.sleep(3)
        self.all_game_rows = self.wd.find_elements(By.CLASS_NAME, 'weeklytopsellers_TableRow_2-RN6')
        game_str = []

        for obj in self.all_game_rows:
            game_str.append(str(obj.text).split('\n'))

        print(len(game_str))    
        for game in game_str:
            pattern = r"[^a-zA-Z0-9\s]"
            flag = 0

            if "Free To Play" in game:
                flag = 1

            self.games.append([game[0], re.sub(pattern, "", game[1]), flag])

            # RANK, GAME NAME  FREE TO PLAY

        if len(self.games) != 100:
            print("ERROR: Did not get 100 games")

    def get_games_appid(self):
        elements = self.wd.find_elements(By.CLASS_NAME, 'weeklytopsellers_TopChartItem_2C5PJ')
        for element in elements:
            extracted_url = element.get_attribute('href')
            appid = extracted_url.split('/')[4]
            self.games_appid.append(appid)

        if len(self.games_appid) != 100:
            print("ERROR: Did not get 100 games url")  

    def get_dataframe(self):
        df = pd.DataFrame(self.games, columns=['Rank', 'Game Name', 'Free to Play'])
        df['App ID'] = self.games_appid
        df['Collection Date'] = self.collection_data
    
        #df_dict = df.to_dict(orient='records')
        #df.to_csv(f'../data/weekly_data/{self.collection_data}_weekly_top_sellers.csv', index=False)     
        return df 
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

    def get_top_10_news(self):
        for app_id in self.games_appid[:10]:
            app_news_url = self.news_url + app_id + "&count=10&maxlength=30000&format=json"
            self.wd.get(app_news_url)
            self.wd.implicitly_wait(10)
            data = self.wd.page_source
            soup = BeautifulSoup(data, 'html.parser')
            pre_tag = soup.find('pre')
            self.news_data = pre_tag.text if pre_tag else None
            self.publish_message(self.producer,self.news_topic,json.dumps(json.loads(self.news_data)))
            
           

                      
    
    def get_reviews(self,app_id,params):
        
        self.response = requests.get(url=self.reviews_url+app_id,params=params).json()
        return self.response
    
        
    def get_positive_reviews(self,app_id,count):
        reviews = []
        params={
            'json':1,
            'filter' : 'recent',
            'review_type' : 'positive'
        }
        self.response = self.get_reviews(app_id,params)
        reviews=reviews +self.response['reviews']
        return reviews 
    def get_negative_reviews(self,app_id,count):
        reviews = []
        params={
            'json':1,
            'filter' : 'recent',
            'review_type' : 'negative'
        }
       
        self.response = self.get_reviews(app_id,params)
        reviews=reviews +self.response['reviews']
        return reviews 
    
    def get_top_10_games_reviews(self):
    
        for app_id in self.games_appid[:10]:

            self.positive_reviews = self.get_positive_reviews(app_id,20)
            
        
            for item in self.positive_reviews:
                #message_dict={'app_id':app_id,'review':item['review'],'voted_up' :item['voted_up']  }
                cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', item['review'].strip())
                self.reviews.append([app_id,cleaned_text,'pos'])
            
            self.negative_reviews= self.get_negative_reviews(app_id,20)
            for item in self.negative_reviews:
                #review_list.append({'review':item['review'],'voted_up' :item['voted_up']  })    
                cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', item['review'].strip())
                self.reviews.append([app_id,cleaned_text,'neg'])
            
    def get_results(self):
        self.get_data()  
        self.get_games()
        self.get_games_appid()
        results=self.get_dataframe()
        results_json = results.to_json(orient='records')
        print(results_json)
        self.producer = self.kafka_producer()
        self.publish_message(self.producer,self.topic, results_json)
        self.get_top_10_games_reviews()
        self.publish_message(self.producer,self.review_topic, json.dumps(self.reviews))
        self.get_top_10_news()
        self.publish_message(self.producer,'close_consumer', json.dumps("END_OF_STREAM"))
        
        self.producer.flush()
        self.producer.close()
        
      
       
        
  

if __name__ == "__main__":
    obj = WeeklyTopSellers()
    obj.get_results()
    #obj.get_consumer()
    obj.wd.quit()
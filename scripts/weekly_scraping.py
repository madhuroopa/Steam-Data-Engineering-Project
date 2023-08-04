from selenium import webdriver
from selenium.webdriver.common.by import By
from WebDriverCreation import WebDriverCreation
import pandas as pd
import re
import time
import datetime
from bs4 import BeautifulSoup
import requests
class WeeklyTopSellers:

    def __init__(self):
        self.all_game_rows = None
        self.games = []
        self.games_appid = []
        self.collection_data = None
        self.driver_instance=WebDriverCreation()
        self.wd=self.driver_instance.wd
        self.base_url = "https://store.steampowered.com/charts/topsellers/US"
        self.news_url = "http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid="
        self.reviews_url = 'https://store.steampowered.com/appreviews/'
        self.positive_reviews=[]
        self.negative_reviews=[]
        
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
        df.to_csv(f'../data/weekly_data/{self.collection_data}_weekly_top_sellers.csv', index=False)     
    def get_top_10_news(self):
        for app_id in self.games_appid[:10]:
            app_news_url = self.news_url + app_id + "&count=10&maxlength=30000&format=json"
            self.wd.get(app_news_url)
            self.wd.implicitly_wait(10)
            data = self.wd.page_source
            soup = BeautifulSoup(data, 'html.parser')
            pre_tag = soup.find('pre')
            json_data = pre_tag.text if pre_tag else None

            if json_data:
                with open(f'../data/weekly_data/{self.collection_data}_news_{app_id}.json', 'w', encoding='utf-8') as json_file:
                    json_file.write(json_data)
                print("Data saved successfully as JSON.")
            else:
                print("Failed to retrieve valid JSON data. Check the URL and API response.")            
    
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
            review_list=[]
            for item in self.positive_reviews:
                review_list.append({'review':item['review'],'voted_up' :item['voted_up']  })    
            
            self.negative_reviews= self.get_negative_reviews(app_id,20)
            for item in self.negative_reviews:
                review_list.append({'review':item['review'],'voted_up' :item['voted_up']  })    
            
            reviews_df = pd.DataFrame(review_list)
            reviews_df.to_csv(f'../data/weekly_data/reviews/{self.collection_data}_{app_id}.csv', index=False)     
        return reviews_df
            
            
                    
    def get_results(self):
        self.get_data()  
        self.get_games()
        self.get_games_appid()
        self.get_dataframe()
        self.get_top_10_games_reviews()

if __name__ == "__main__":
    obj = WeeklyTopSellers()
    obj.get_results()
    obj.wd.quit()

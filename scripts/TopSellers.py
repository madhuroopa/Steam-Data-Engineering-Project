from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from WebDriverCreation import WebDriverCreation
import pandas as pd
import re
import time
import datetime

class TopSellers:
    def __init__(self):
        self.games_list=[]
        self.collection_date = pd.to_datetime('today').strftime("%Y-%m-%d")
        self.driver_instance=WebDriverCreation()
        self.wd=self.driver_instance.wd
        self.url='https://store.steampowered.com/search/?filter=topsellers'
        
    def scroll_page(self, wd):
        '''Takes the driver as an input and simulates the scrolling to get all the games'''  
        SCROLL_PAUSE_TIME = 2
        last_height = wd.execute_script("return document.body.scrollHeight")

        for i in range(6):
    # Scroll down to bottom
            wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # Wait to load page
            time.sleep(SCROLL_PAUSE_TIME)


            # Calculate new scroll height and compare with last scroll height
            new_height = wd.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_data(self):
        self.wd.get(self.url)
        self.scroll_page(self.wd)
        print(self.wd.title)
    
    def get_games_list(self):
        games= self.wd.find_element(By.ID, 'search_resultsRows')
        games_rows = games.find_elements(By.TAG_NAME,'a')
        for i in range(len(games_rows)):
            title =  games_rows[i].find_element(By.CLASS_NAME, "title").text
            game_url = games_rows[i].get_attribute('href')
    
            #storing the extracted information inside a dictionary
            my_game = {
                'title': title,
                'url': game_url,
            }

             #adding the dictionary inside the list
            self.games_list.append(my_game)
        return self.games_list
    def check_page(self,wd):
        try: 
            try:
                info_tag = wd.find_element(By.CLASS_NAME, 'glance_ctn_responsive_left')
                return wd
            except:
                year_tag = wd.find_element(By.CLASS_NAME, 'agegate_birthday_selector')
                year = year_tag.find_element(By.ID, 'ageYear')
                yearDD = Select(year)
                yearDD.select_by_value('1900')
                view_button = wd.find_element(By.XPATH, '//*[@id="view_product_page_btn"]')
                view_button.click()
                time.sleep(4)
        except:
            return wd

        return wd
    def get_price(self,wd):
        try:  
            try:  
                try:
                    price_tag = wd.find_element(By.CLASS_NAME, 'game_purchase_action')
                    price = price_tag.find_element(By.CLASS_NAME, 'price').text.strip('$')
        
                except:
                    price_tag = wd.find_element(By.CLASS_NAME, 'discount_prices')
                    prices = price_tag.text.strip('\n').split('$')
                    price = prices[1]
        
            except:
                price = 'not availale'

        except:
            price_tag = wd.find_element(By.CLASS_NAME, 'discount_prices')
            price = price_tag.find_element(By.CLASS_NAME, 'discount_original_price').text

        return price
    
    def get_discounted(self,wd):
        try:
            try:  
                try:
                    price_tag = wd.find_element(By.CLASS_NAME, 'game_purchase_action')
                    discprice = price_tag.find_element(By.CLASS_NAME, 'price').text.strip('$')
          
                except:
                    price_tag = wd.find_element(By.CLASS_NAME, 'discount_prices')
                    prices = price_tag.text.strip('\n').split('$')
                    discprice = prices[2]
      
            except:
                discprice = 'not available'

        except:
            price_tag = wd.find_element(By.CLASS_NAME, 'discount_prices')
            discprice = price_tag.find_element(By.CLASS_NAME, 'discount_final_price')

        return discprice
    def get_appid(self,game_url):
        app_id = re.search(r'(\d+)', game_url)
        if app_id:
            app = app_id.group() # Collects app id
        return app
    def get_release(self,wd):
        try: 
            info_tag = wd.find_element(By.CLASS_NAME, 'glance_ctn_responsive_left')

            release = info_tag.find_element(By.CLASS_NAME, 'release_date').text.strip('RELEASE DATE:\n')
        except:
            release = 'not available'
       
        return release
    '''Takes the driver an returns the reviews of the game'''
    '''def get_reviews(self,wd):
        try:  
            info_tag = wd.find_element(By.CLASS_NAME, 'glance_ctn_responsive_left')
            try:
                reviews = info_tag.find_element(By.XPATH, '//*[@id="userReviews"]/div[2]').text.replace('ALL REVIEWS:\n', '')
            except:
                reviews = info_tag.find_element(By.CLASS_NAME, 'user_reviews').text.replace('ALL REVIEWS:\n', '')
        except:
            reviews = 'not available'
        return reviews'''
    
    def get_game_info(self,url):
        wd_new = self.check_page(self.wd)
        appid=self.get_appid(url)
        price = self.get_price(wd_new)
        discounted = self.get_discounted(wd_new)
        release = self.get_release(wd_new)
        #reviews = self.get_reviews(wd_new)
        mygame = {
            'Price': price,
            'Discounted Price': discounted,
            'Release Date': release,
            'App ID': appid,
            #'Reviews': reviews
           }
        return mygame
    def get_all_games(self,new_gamesdf):
        games_url = new_gamesdf['url']
        game_info_list = []
        for i in games_url:
            game_info = self.get_game_info(i)
            game_info_list.append(game_info)
        game_info_df = pd.DataFrame(game_info_list)
        game_info_df ['Execution Date'] = self.collection_date
        return game_info_df
    def merge_dataframe(self,games_df,games_info_df):
        merged_df=pd.concat([games_df,games_info_df],axis=1,join='inner')
        merged_df.reset_index(drop=True, inplace=True)
        merged_df.to_csv(f'../data/daily_data/top_sellers/{self.collection_date}_top_sellers.csv')
        
    
    
    
    
if __name__=="__main__":
    obj = TopSellers()
    obj.get_data()
    games_list=obj.get_games_list()
    games_df= pd.DataFrame(games_list)
    games_info_df=obj.get_all_games(games_df)
    
    print(games_df.head())
    print(games_info_df.head())
    obj.merge_dataframe(games_df,games_info_df)
    obj.wd.quit()
    
    
            
        
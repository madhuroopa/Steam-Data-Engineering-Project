from selenium import webdriver
from selenium.webdriver.common.by import By
from WebDriverCreation import WebDriverCreation
import pandas as pd
import re
import time
import datetime


class MostPlayedGames:
    def __init__(self) -> None:
        
        self.driver_instance=WebDriverCreation()
        self.wd=self.driver_instance.wd
        self.url="https://store.steampowered.com/charts/mostplayed"
        self.games_list=[]
        self.games=[]
        self.collection_date=pd.to_datetime('today').strftime("%Y-%m-%d")
    
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
        #Takes the driver as an input gives and returns a selenium web element with all the games'''
    def get_games(self):
        game_rows = self.wd.find_elements(By.CLASS_NAME, 'weeklytopsellers_TableRow_2-RN6')
        #games_rows = wd.find_element(By.ID, 'search_resultsRows')
        #games = game_rows.find_elements(By.TAG_NAME,'tr')
        for game in game_rows:
            self.games_list.append(str(game.text).split('\n'))
        for game in self.games_list:
            flag=0
            if 'Free To Play' in game:
                flag=1
            temp_count=game[-1]
            current_players,peek_today=temp_count.split(" ")
            self.games.append([game[0],game[1],flag,current_players,peek_today])
        return self.games
        
                
    def get_dataframe(self):
        df = pd.DataFrame(self.games, columns=['Rank', 'Game Name', 'Free to Play','Current Players','Peek Today'])
        df['Collection Date'] = self.collection_date
        df.to_csv(f'../data/daily_data/most_played/{self.collection_date}_MostPlayed.csv', index=False)         
            
        
if __name__=="__main__":
    obj=MostPlayedGames()
    obj.get_data()
    games= obj.get_games()
    #print(games)
    obj.get_dataframe()
    
    
 


        
    
    
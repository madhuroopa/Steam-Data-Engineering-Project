from selenium.webdriver.common.by import By
from WebDriverCreation import WebDriverCreation
import pandas as pd
import time


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

        for _ in range(6):
            wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = wd.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height


    def get_data(self):
        self.wd.get(self.url)
        self.scroll_page(self.wd)
        print(self.wd.title)

    def get_games(self):
        game_rows = self.wd.find_elements(By.CLASS_NAME, 'weeklytopsellers_TableRow_2-RN6')
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
    obj.get_dataframe()
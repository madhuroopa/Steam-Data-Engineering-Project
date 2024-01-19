from selenium.webdriver.common.by import By
from WebDriverCreation import WebDriverCreation
import pandas as pd
import time
from kafka import KafkaProducer
import os
import dotenv
dotenv.load_dotenv()

AWS_PUBLIC_IP = os.getenv('AWS_PUBLIC_IP')
PORT = int(os.getenv('PORT'))

class MostPlayedGamesProducer:
    def __init__(self) -> None:
        self.driver_instance = WebDriverCreation()
        self.wd = self.driver_instance.wd
        self.url = "https://store.steampowered.com/charts/mostplayed"
        self.games_list = []
        self.games = []
        self.collection_date = pd.to_datetime('today').strftime("%Y-%m-%d")
        self.kafka_bootstrap_servers = f'{AWS_PUBLIC_IP}:{PORT}'
        self.kafka_topic = 'most_played_games'

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
            flag = 0
            if 'Free To Play' in game:
                flag = 1
            temp_count = game[-1]
            current_players, peek_today = temp_count.split(" ")
            current_players = current_players.replace(',', '')
            current_players = current_players.replace('"', '')
            peek_today = peek_today.replace(',', '')
            peek_today = peek_today.replace('"', '')
            game[1] = game[1].replace(',', '_')
            self.games.append([str(game[0]), game[1], flag, current_players, peek_today])

    def produce_to_kafka(self):
        producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
        for game in self.games:
            message = ','.join(str(field) for field in game).encode('utf-8')
            producer.send(self.kafka_topic, value=message)
            print("Message sent to Kafka")
        producer.send(self.kafka_topic, "END_OF_STREAM".encode('utf-8')) 
        producer.flush()
        producer.close()

    def runner(self):
        self.get_data()
        self.get_games()
        self.produce_to_kafka()    

if __name__ == "__main__":
    obj = MostPlayedGamesProducer()
    obj.runner()
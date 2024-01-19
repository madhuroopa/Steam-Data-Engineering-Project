from kafka import KafkaConsumer
import pandas as pd
import time
import os
import dotenv
dotenv.load_dotenv()

LOCAL_IP = os.getenv('LOCAL_IP')
PORT = int(os.getenv('PORT'))

class MostPlayedGamesConsumer:
    def __init__(self) -> None:
        self.kafka_bootstrap_servers = f'{LOCAL_IP}:{PORT}'
        self.kafka_topic = 'most_played_games'
        self.collection_date = pd.to_datetime('today').strftime("%Y-%m-%d")

    def consume_from_kafka(self):
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            auto_offset_reset='latest',
            group_id=None
        )
        games = []
        for message in consumer:
            if message.value == b"END_OF_STREAM":
                print("Received end-of-stream message. Stopping the consumer.")
                break

            game_data = message.value.decode('utf-8').split(',')
            games.append(game_data)
            print(f"Message consumed from Kafka")
            
        consumer.close()
        return games

    def save_as_csv(self, games):
        df = pd.DataFrame(games, columns=['Rank', 'Game Name', 'Free to Play', 'Current Players', 'Peek Today'])
        df['Collection Date'] = self.collection_date
        df.to_csv(f'../../data/daily_data/most_played/{self.collection_date}_MostPlayed_Consumed.csv', index=False)

    def runner(self):
        print("Starting the consumer.")
        games = self.consume_from_kafka()
        self.save_as_csv(games)

##Uncomment if running without Airflow        
# if __name__ == "__main__":
#     obj = MostPlayedGamesConsumer()
#     obj.runner()

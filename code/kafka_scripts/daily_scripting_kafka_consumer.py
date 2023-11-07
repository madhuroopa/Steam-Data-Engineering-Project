from kafka import KafkaConsumer
import pandas as pd
import time
import boto3
import json
from io import StringIO
class MostPlayedGamesConsumer:
    def __init__(self) -> None:
        self.kafka_bootstrap_servers = '54.196.241.7:9092'
        self.kafka_topic = 'most_played_games'
        self.collection_date = pd.to_datetime('today').strftime("%Y-%m-%d")
        self.s3 = boto3.client('s3')
        self.BUCKET_NAME="steam-processing-madhu"
        self.read_config()
	
    def read_config(self):
        """with open('./aws_config.json',"r") as json_file:
            #config_data=json.load(json_file)
            #self.AWS_ACCESS_KEY_ID=config_data["aws_access_key_id"]
            #self.AWS_SECRET_KEY=config_data["aws_secret_access_key"]
            #self.AWS_REGION=config_data["aws_region"]
            #self.BUCKET_NAME=config_data["s3_bucket_name"]
             #config_data=json.load(json_file)

            self.AWS_ACCESS_KEY_ID="AKIA5NZYECEMMMNCA6FV"
            self.AWS_SECRET_KEY="lL2ReDe/ibxO/PJ2xWc0bsA4+HWnWEdRRhin2tt"
            self.AWS_REGION="us-east-1"
            self.BUCKET_NAME="steam-processing-madhu"
        #self.s3 = boto3.client('s3', aws_access_key_id=self.AWS_ACCESS_KEY_ID, aws_secret_access_key=self.AWS_SECRET_KEY, region_name=self.AWS_REGION)
        session= boto3.Session(aws_access_key_id=self.AWS_ACCESS_KEY_ID, aws_secret_access_key=self.AWS_SECRET_KEY)
        self.s3=session.resource("s3")"""

            
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
        #with self.s3.open("s3://steam-processing-madhu/raw_data/daily_data/most_played/{self.collection_date}_MostPlayed_Consumed.csv")
        path=f'../../data/daily_data/most_played/{self.collection_date}_MostPlayed_Consumed.csv'
        #s3_key ='/data/daily_data/test.csv'
        s3_key =f'/data/daily_data/most_played/{self.collection_date}_MostPlayed_Consumed.csv'
        self.s3.upload_file(path, self.BUCKET_NAME, s3_key)
        
      
    def runner(self):
        print("Starting the consumer.")
        games = self.consume_from_kafka()
        self.save_as_csv(games)
if __name__ == "__main__":
    obj = MostPlayedGamesConsumer()
    obj.runner()

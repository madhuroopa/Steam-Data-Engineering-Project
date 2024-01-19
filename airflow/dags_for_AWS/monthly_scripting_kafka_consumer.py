from kafka import KafkaConsumer
import json
from datetime import date

import dotenv
import os
dotenv.load_dotenv()

AWS_PUBLIC_IP = os.getenv('AWS_PUBLIC_IP')
PORT = os.getenv('PORT')

class MonthlyScrapingConsumer:
    def __init__(self):
        self.today = date.today()
        self.kafka_broker = f'{AWS_PUBLIC_IP}:{PORT}'
        self.kafka_topic = 'monthly_visitor_data'
        self.consumer = KafkaConsumer(self.kafka_topic, bootstrap_servers=self.kafka_broker, 
                                value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    def kafka_consumer(self):
        for message in self.consumer:
            data = message.value
            if data == "END_OF_STREAM":  
                print("End of stream message received. Exiting the consumer.")
                break

            print("Received data from Kafka topic:")
            with open(f'../../data/monthly_data/{self.today.strftime("%Y-%m-%d")}_monthly_visitor_data.json', 'w') as json_file:
                json.dump(data, json_file, indent=4)
            print(f"Data saved as JSON: monthly_visitor_data_{message.timestamp}.json")
            
    def runner(self):
        self.kafka_consumer()
        self.consumer.close()

# if __name__ == '__main__':
#     monthly_scraping_consumer = MonthlyScrapingConsumer()
#     monthly_scraping_consumer.runner()
from kafka import KafkaConsumer
import json
from datetime import date
today = date.today()
import dotenv
import os
dotenv.load_dotenv()

AWS_PUBLIC_IP = os.getenv('AWS_PUBLIC_IP')
PORT = os.getenv('PORT')

def kafka_consumer():
    kafka_broker = f'{AWS_PUBLIC_IP}:{PORT}'
    kafka_topic = 'monthly_visitor_data'

    consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_broker, 
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        data = message.value
        if data == "END_OF_STREAM":  
            print("End of stream message received. Exiting the consumer.")
            break

        print("Received data from Kafka topic:")
        with open(f'../../data/monthly_data/{today.strftime("%Y-%m-%d")}_monthly_visitor_data.json', 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data saved as JSON: monthly_visitor_data_{message.timestamp}.json")
        consumer.close()

if __name__ == '__main__':
    kafka_consumer()

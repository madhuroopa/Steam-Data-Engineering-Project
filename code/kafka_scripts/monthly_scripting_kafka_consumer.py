from kafka import KafkaConsumer
import json
from datetime import date
import boto3
from io import StringIO
today = date.today()

def kafka_consumer():
    kafka_broker = '54.242.137.34:9092'
    kafka_topic = 'monthly_visitor_data_v1'
    s3=boto3.client('s3')
    BUCKET_NAME="steam-processing-madhu"
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
        path =f'../../data/monthly_data/{today.strftime("%Y-%m-%d")}_monthly_visitor_data.json'
        s3_key = f'data/monthly_data/{today.strftime("%Y-%m-%d")}_monthly_visitor_data.json'
        s3.upload_file(path,BUCKET_NAME, s3_key)
        print(f"Data saved as JSON: {s3_key}")
        consumer.close()

if __name__ == '__main__':
    kafka_consumer()

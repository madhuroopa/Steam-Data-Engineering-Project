from kafka import KafkaConsumer
import pandas as pd
from datetime import date
import json
collection_data = date.today()

consumer = KafkaConsumer(
    'weekly_top_sellers_games',
    'weekly_top_sellers_app_id',
    'weekly_reviews',
    'weekly_news',
    'close_consumer',
    bootstrap_servers='localhost:9092',
    group_id='weekly_data_group',
    auto_offset_reset='earliest',   
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,  
    )

i = 0
for message in consumer:
    topic = message.topic
    data = message.value.decode('utf-8')
    
    if topic == 'weekly_top_sellers_games':
        game_list = []
        print(f"Received top sellers data")
        for rows in eval(data):
            game_list.append(rows)

        df = pd.DataFrame(game_list, columns=['Rank', 'Game Name', 'Free to Play'])
        df.to_csv(f'../data/weekly_data/top_sellers/{collection_data}_weekly_top_sellers.csv', index=False)
    
    elif topic == 'weekly_top_sellers_app_id':
        print(f"Received app ids")
        appid_list = []
        for rows in eval(data):
            appid_list.append(rows)
        df = pd.DataFrame(appid_list, columns=['App ID'])
        df.to_csv(f'../data/weekly_data/top_sellers/{collection_data}_weekly_top_sellers_appIds.csv', index=False)

    elif topic == 'weekly_reviews':
        print("Reviews received")
        review_list = []
        for row in eval(data):   
            review_list.append(row)
        df = pd.DataFrame(review_list, columns=['App ID', 'Review', 'Voted Up'])
        df.to_csv(f'../data/weekly_data/reviews/{collection_data}_weekly_reviews.csv', index=False)        

    elif topic == 'weekly_news':
        print("News received")
        with open(f'../data/weekly_data/news/{collection_data}_news_{i}.json', 'w') as json_file:
            json.dump(json.loads(data), json_file, indent=4)
            i += 1     

    elif topic == 'close_consumer':
        print("Closing consumer")
        consumer.close()          
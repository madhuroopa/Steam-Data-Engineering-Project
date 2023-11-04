from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import length, col, count, expr, monotonically_increasing_id, lit

import pandas as pd
import json
import re
import os

class WeeklyScript:
    def __init__(self):
        self.spark = SparkSession.builder.appName('weekly_spark').getOrCreate()
        self.FILE_DATE = None

        self.reviews_schema = StructType([
            StructField("App ID", IntegerType(), True),
            StructField("Review", StringType(), True),
            StructField("Voted Up", StringType(), True)
        ])

        self.top_sellers_schema = StructType([
            StructField("Rank", IntegerType(), True),
            StructField("Game Name", StringType(), True),
            StructField("Free to Play", IntegerType(), True),
            ])

        self.top_sellers_appids_schema = StructType([
            StructField("App ID", IntegerType(), True),
        ])
        
    def read_file(self):    
        self.WEEKLY_DATA_PATH = r'../../data/weekly_data/'
        reviews_path = os.path.join(self.WEEKLY_DATA_PATH, 'reviews/')

        WEEKLY_TOP_SELLERS_PATH = self.WEEKLY_DATA_PATH + r'top_sellers/'
        top_sellers_files = os.listdir(WEEKLY_TOP_SELLERS_PATH)

        try:
            # Reviews
            csv_files = [f for f in os.listdir(reviews_path) if f.endswith('.csv')]
            if csv_files:
                csv_file = csv_files[0]
                self.FILE_DATE = csv_file[0].split('.')[0].split('_')[0]
                csv_file_path = os.path.join(reviews_path, csv_file)
                self.most_daily_played = self.spark.read.csv(csv_file_path, header=True, schema=self.reviews_schema)
            else:
                print("No CSV files found in the 'reviews_path' directory.")

            #Top_sellers
            csv_file1 = [f for f in top_sellers_files if f.endswith('weekly_top_sellers.csv')]
            self.FILE_DATE = csv_file1[0].split('.')[0].split('_')[0]
            self.top_sellers_games = self.spark.read.csv(
                WEEKLY_TOP_SELLERS_PATH + csv_file1[0],
                header=True,
                schema=self.top_sellers_schema  
            )
            csv_file2 = [f for f in top_sellers_files if f.endswith('weekly_top_sellers_appIds.csv')]
            self.FILE_DATE = csv_file2[0].split('.')[0].split('_')[0]
            self.top_sellers_appids = self.spark.read.csv(
                WEEKLY_TOP_SELLERS_PATH + csv_file2[0],
                header=True,
                schema=self.top_sellers_appids_schema  
            )

            #news
            self.WEEKLY_NEWS_PATH = self.WEEKLY_DATA_PATH + r'news/'
            weekly_news_files = os.listdir(self.WEEKLY_NEWS_PATH)
            self.dfs = []

            for file in weekly_news_files:
                with open(self.WEEKLY_NEWS_PATH + file, 'r') as json_file:
                    json_data = json.load(json_file)

                    df = pd.DataFrame() 
                    for i in range(len(json_data['appnews']['newsitems'])):
                        column_name = f"contents_{i}" 
                        value = json_data['appnews']['newsitems'][i]['contents']
                        if isinstance(value, (list, dict)):
                            value = str(value)
                        df[column_name] = [value]
                    df['App ID'] = json_data['appnews']['appid']
                    self.dfs.append(df)
                
        except Exception as e:
            print("An error occurred while reading the CSV file:", e)

    def clean_data(self):
        # Cleaning the data
        self.most_daily_played = self.most_daily_played.na.drop(subset=["Review", "Voted Up", "App ID"])
        self.most_daily_played = self.most_daily_played.filter(length(col("Review")) >= 2)

        #news
        self.news_df = pd.concat(self.dfs, ignore_index=True)
        self.columns_to_iterate = self.news_df.columns[:-1]
        
    def filter_data(self):
        #reviews
        # Counting the number of positive and negative reviews
        self.counted_reviews = self.most_daily_played.groupBy("App ID").pivot("Voted Up", ["pos", "neg"]).agg(count("*").alias("count"))

        # Seprarating the positive and negative reviews
        self.neg_reviews_df = self.most_daily_played.filter(self.most_daily_played["Voted Up"] == "neg")
        self.pos_reviews_df = self.most_daily_played.filter(self.most_daily_played["Voted Up"] == "pos")

        self.neg_reviews_df = self.neg_reviews_df.withColumn("FILE_DATE", lit(self.FILE_DATE))
        self.pos_reviews_df = self.pos_reviews_df.withColumn("FILE_DATE", lit(self.FILE_DATE))
        self.counted_reviews = self.counted_reviews.withColumn("FILE_DATE", lit(self.FILE_DATE))

        #top_sellers
        self.top_sellers_appids = self.top_sellers_appids.withColumn(
                                                "Rank",(monotonically_increasing_id() + 1).cast("int"))
        self.top_sellers = self.top_sellers_games.join(self.top_sellers_appids, on=["Rank"], how="inner")
        self.top_sellers = self.top_sellers.withColumn("FILE_DATE", lit(self.FILE_DATE))    

        #news
        for column in self.columns_to_iterate:
            for index, cell in enumerate(self.news_df[column]):
                if cell is not None:
                    cleaned_text = re.sub(r'<strong>|</strong>|<a>|</a>|<a\s+href\s*=\s*".*?"\s*>\s*|https://.*|\{STEAM_CLAN_IMAGE\}.*', '', cell)
                    self.news_df.at[index, column] = cleaned_text

        self.news_df.to_csv(r'../../data/weekly_data/news/spark_modified_news.csv', index=False, sep='\t')
        self.news_path = os.path.join(self.WEEKLY_DATA_PATH, 'news/')

        try:
            csv_files = [f for f in os.listdir(self.news_path) if f.endswith('.csv')]
            if csv_files:
                csv_file = csv_files[0]
                self.FILE_DATE = csv_file.split('.')[0].split('_')[0]  
                csv_file_path = os.path.join(self.news_path, csv_file)
                self.news_spark_df = self.spark.read.option("delimiter", "\t").csv(csv_file_path, header=True)
            else:
                print("No CSV files found in the 'news_path' directory.")
        except Exception as e:
            print("An error occurred while reading the CSV file:", e)
                        

    def save_filtered_data(self):
        neg_reviews_path = r"../../cleaned_data/weekly_data/neg_reviews"
        pos_reviews_path = r"../../cleaned_data/weekly_data/pos_reviews"
        counted_reviews_path = r"../../cleaned_data/weekly_data/counted_reviews"
        top_sellers_path = r"../../cleaned_data/weekly_data/top_sellers"
        news_spark_path = r"../../cleaned_data/weekly_data/news_spark_df"

        # Save the DataFrame as CSV
        self.neg_reviews_df.write.format("csv").mode("overwrite").option("header", "true").save(neg_reviews_path)
        self.pos_reviews_df.write.format("csv").mode("overwrite").option("header", "true").save(pos_reviews_path)
        self.counted_reviews.write.format("csv").mode("overwrite").option("header", "true").save(counted_reviews_path)
        self.top_sellers.write.format("csv").mode("overwrite").option("header", "true").save(top_sellers_path)
        self.news_spark_df.write.format("csv").mode("overwrite").option("header", "true").save(news_spark_path)

    def stop_spark(self):
        self.spark.stop()

    def runner(self):
        self.read_file()
        self.clean_data()
        self.filter_data()
        self.save_filtered_data()
        self.stop_spark()
        print("Weekly Script Completed")    

if __name__ == "__main__":
    weekly_script = WeeklyScript()
    weekly_script.runner()        
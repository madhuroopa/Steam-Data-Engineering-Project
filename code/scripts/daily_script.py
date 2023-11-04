from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.functions import regexp_replace, col
import os

class DailyScript:
    def __init__(self):
        self.spark = SparkSession.builder.appName('daily_spark').getOrCreate() 
        self.schema = StructType([
                        StructField("Rank", IntegerType(), True),
                        StructField("Game Name", StringType(), True),
                        StructField("Free to Play", IntegerType(), True),
                        StructField("Current Players", IntegerType(), True),
                        StructField("Peek Today", IntegerType(), True),
                        StructField("Collection Date", DateType(), True)
                    ])  # Define schema for our data using DDL
        self.FILE_DATE = None

    def read_file(self):
        DAILY_DATA_PATH = r'../../data/daily_data/most_played/' 
        files = os.listdir(DAILY_DATA_PATH)

        try:
            csv_file = [f for f in files if f.endswith('.csv')]
            file = csv_file[0]
            self.most_daily_played = self.spark.read.csv(DAILY_DATA_PATH + file, header=True, schema=self.schema)
        except Exception as e:
            print("An error occurred while reading the JSON file:", e)  

    def clean_data(self):
        special_characters = ["™", "®"]
        for char in special_characters:
            self.most_daily_played = self.most_daily_played.withColumn("Game Name", regexp_replace(col("Game Name"), char, ""))

        self.most_daily_played.cache()

    def filter_data(self):
        # Filter free to play games and create a new DataFrame
        free_to_play_df = self.most_daily_played.filter(self.most_daily_played["Free to Play"] == 1)
        not_free_to_play_df = self.most_daily_played.filter(self.most_daily_played["Free to Play"] == 0)

        # Sort by Peek Today
        self.free_to_play_sorted = free_to_play_df.orderBy("Peek Today")
        self.not_free_to_play_sorted = not_free_to_play_df.orderBy("Peek Today")   

    def save_filtered_data(self):
        path_top_20 = r"../../cleaned_data/daily_data/top_20"
        path_top_free = r"../../cleaned_data/daily_data/top_free"
        path_top_not_free = r"../../cleaned_data/daily_data/top_not_free"

        # Save the DataFrame as CSV
        self.most_daily_played.write.format("csv").mode("overwrite").option("header", "true").save(path_top_20)
        import os
        print("SAVING AT:", os.getcwd())
        self.free_to_play_sorted.write.format("csv").mode("overwrite").option("header", "true").save(path_top_free)
        print("SAVING AT:", os.getcwd())
        self.not_free_to_play_sorted.write.format("csv").mode("overwrite").option("header", "true").save(path_top_not_free) 
        print("SAVING AT:", os.getcwd())
        print("Current Files in the directory:", os.listdir(os.getcwd()))

    def stop_spark(self):
        self.spark.stop()

    def runner(self):
        self.read_file()
        self.clean_data()
        self.filter_data()
        self.save_filtered_data()
        self.stop_spark()
        print("Daily Script Completed")

# if __name__ == "__main__":
#     daily_script_obj = DailyScript()
#     daily_script_obj.runner()
    # run this code aft u return 
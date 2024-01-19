from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, MapType, ArrayType
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
import os


class MonthlyScript:
    def __init__(self):
        self.spark = SparkSession.builder.appName('monthly_spark').getOrCreate() 
        self.FILE_DATE = None

        # Define schema for the data
        self.schema = StructType([
            StructField("Version", IntegerType(), True),
            StructField("SiteName", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("TopCountryShares", ArrayType(StructType([
                StructField("Country", IntegerType(), True),
                StructField("CountryCode", StringType(), True),
                StructField("Value", DoubleType(), True),
            ])), True),
            StructField("Title", StringType(), True),
            StructField("Engagments", StructType([
                StructField("BounceRate", StringType(), True),
                StructField("Month", StringType(), True),
                StructField("Year", StringType(), True),
                StructField("PagePerVisit", StringType(), True),
                StructField("Visits", StringType(), True),
                StructField("TimeOnSite", StringType(), True),
            ]), True),
            StructField("EstimatedMonthlyVisits", MapType(StringType(), IntegerType()), True),
            StructField("GlobalRank", StructType([
                StructField("Rank", IntegerType(), True),
            ]), True),
            StructField("CountryRank", StructType([
                StructField("Country", StringType(), True),
                StructField("CountryCode", StringType(), True),
                StructField("Rank", IntegerType(), True),
            ]), True),
            StructField("CategoryRank", StructType([
                StructField("Rank", IntegerType(), True),
                StructField("Category", StringType(), True),
            ]), True),
            StructField("IsSmall", BooleanType(), True),
            StructField("Policy", IntegerType(), True),
            StructField("TrafficSources", StructType([
                StructField("Social", DoubleType(), True),
                StructField("Paid Referrals", DoubleType(), True),
                StructField("Mail", DoubleType(), True),
                StructField("Referrals", DoubleType(), True),
                StructField("Search", DoubleType(), True),
                StructField("Direct", DoubleType(), True),
            ]), True),
            StructField("Category", StringType(), True),
            StructField("LargeScreenshot", StringType(), True),
            StructField("IsDataFromGa", BooleanType(), True),
            StructField("Countries", ArrayType(StructType([
                StructField("Code", StringType(), True),
                StructField("UrlCode", StringType(), True),
                StructField("Name", StringType(), True),
            ])), True),
            StructField("Competitors", StructType([
                StructField("TopSimilarityCompetitors", ArrayType(StructType([
                    StructField("Domain", StringType(), True),
                ])), True),
            ]), True),
        ])

    def read_file(self):
        MONTHLY_DATA_PATH = r'../../data/monthly_data/' 
        files = os.listdir(MONTHLY_DATA_PATH)

        try:
            json_file = [f for f in files if f.endswith('.json')]
            file = json_file[0]
            self.FILE_DATE = file.split('.')[0].split('_')[0]
            self.steam_traffic = self.spark.read.json(
                MONTHLY_DATA_PATH + file,
                multiLine=True,
                schema=self.schema  
            )
            self.steam_traffic.cache()
            self.steam_traffic = self.steam_traffic.withColumn("FILE_DATE", lit(self.FILE_DATE))
        except Exception as e:
            print("An error occurred while reading the JSON file:", e)

    def filter_data(self):
        self.engagements_data = self.steam_traffic.select("Engagments.BounceRate",
                                        "Engagments.Month",
                                        "Engagments.Year",
                                        "Engagments.PagePerVisit",
                                        "Engagments.Visits",
                                        "Engagments.TimeOnSite")

        self.engagements_data = self.engagements_data.withColumn("FILE_DATE", lit(self.FILE_DATE))
        self.estimated_monthly_visits = self.steam_traffic.select("EstimatedMonthlyVisits")
        # Split the 'EstimatedMonthlyVisits' column into separate rows and columns
        self.estimated_monthly_visits = self.estimated_monthly_visits.select(
            explode("EstimatedMonthlyVisits").alias("Date", "Visits")
        )
        self.estimated_monthly_visits = self.estimated_monthly_visits.withColumn("FILE_DATE", lit(self.FILE_DATE))

        self.traffic_sources = self.steam_traffic.select("TrafficSources.Social",
                                       "TrafficSources.Paid Referrals",
                                       "TrafficSources.Mail",
                                       "TrafficSources.Referrals",
                                       "TrafficSources.Search",
                                       "TrafficSources.Direct")

        self.traffic_sources = self.traffic_sources.withColumn("FILE_DATE", lit(self.FILE_DATE)) 

    def save_filtered_data(self):
        engagements_path = r"../../cleaned_data/monthly_data/engagements_data"
        traffic_sources_path = r"../../cleaned_data/monthly_data/traffic_sources"
        estimated_monthly_visits_path = r"../../cleaned_data/monthly_data/estimated_monthly_visits"

        # Save the DataFrame as CSV
        self.engagements_data.write.format("csv").mode("overwrite").option("header", "true").save(engagements_path)
        self.traffic_sources.write.format("csv").mode("overwrite").option("header", "true").save(traffic_sources_path)
        self.estimated_monthly_visits.write.format("csv").mode("overwrite").option("header", "true").save(estimated_monthly_visits_path)


    def stop_spark(self):
        self.spark.stop()

    def runner(self):
        self.read_file()
        self.filter_data()
        self.save_filtered_data()
        self.stop_spark()
        print("Monthly Script Completed")

##Uncomment if running without Airflow
# if __name__ == "__main__":
#     monthly_script_obj = MonthlyScript()
#     monthly_script_obj.runner()
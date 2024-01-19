# Steam-Data-Engineering-Project

## Project Overview
Welcome to the heart of real-time data engineering—our project dedicated to unraveling the gaming wonders of [Steam](https://store.steampowered.com/). As one of the paramount digital distribution platforms for PC gaming, Steam sets the stage for our data orchestration. Brace yourself for a journey powered by Kafka, Spark, Airflow, and AWS, where I would perform data `Extraction, Transformation, and Loading` (ETL).

## Diagram

## Kafka Spotlight 🌟
Hey there, data enthusiast! Let's shine a light on Kafka, the backbone of data collection. To use Kafka, I have set up a simple <i>producer-consumer</i> schema for each web page. The producer scrapes the web page or collects data through Steam's APIs. This data is consumed by a consumer who then stores the data accordingly.

## The Pipeline Trio 🚀
Three pipelines, three different cadences—daily, weekly, and monthly. This setup ensures a separation of concerns and a steady flow of fresh data. 

### Daily Rhythms 🌅
- Source:
    - [Most Played Games](https://store.steampowered.com/charts/mostplayed)
- Purpose: Collect daily data on the most played games.

Every day, the curtain rises on the gaming stage. Behold the Most Played Games list from Steam. With the finesse of web scraping and the power of Kafka, the Daily Kafka Producer takes the spotlight, bringing the latest player counts, game ranks, and more to the forefront. 

### Weekly Data Wonders 🌈
- Sources:
    - [Top Sellers in the US](https://store.steampowered.com/charts/topsellers/US)
    - [Steam News](http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid=)
    - [App Reviews](https://store.steampowered.com/appreviews/)
- Purpose: Gathers weekly insights by aggregating data from top sellers, game news, and user reviews.    

Our Weekly Data Wonders unfold from the WEEKLY TOP SELLERS, showcasing the top 100 games in revenue. Armed with App IDs, it delves into game news for updates and bug fixes straight from the developers. Simultaneously, I also tap into the community's heartbeat—user reviews from the app page, offering a valuable pulse on user sentiments.

### Monthly Data Marvels 🚀
- Source: 
    - [Monthly Visits](https://data.similarweb.com/api/v1/data?domain=store.steampowered.com)
- Purpose: Collects monthly data related to network traffic, page visits, and engagement metrics, enriching our understanding of Steam's audience.

Powered by Steam's API, the  Monthly Data Marvels unveil a backstage pass to Steam's audience spectacle. Network traffic sources, page visits, page hops, and other engagement metrics paint a vibrant canvas, helping us decipher the diverse audience that flocks to Steam.


* <i> Note: Although I wanted to collect more data to process, the other options are paid, but they provide great insights. If you want to have an intense collection of data, you can refer to this [link](https://www.similarweb.com/) </i>

## PySpark and Airflow Data Symphony 🦄
I use PySpark to process the data seamlessly. The magic doesn't stop there— Airflow joins in, orchestrating the entire data flow with its slick Directed Acyclic Graphs (DAGs). 

### Local and Cloud Vibes ☁️🖥️
The project is versatile and ready to run on both- local machines and in the expansive AWS cloud. Let's dive into the execution intricacies.

When running locally the data from Kafka Consumer is stored inside a data folder in the following [structure](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/tree/main/data). 
If running on AWS, the data is stored in an S3 bucket named `steam-project-raw` 

Once raw data is stored, I also have a shell script that runs to create a backup of the raw data.
Note: This would be triggered by Airflow later. The backup script creates a copy of this raw data and stores it locally or on an S3 bucket named `steam-projet-raw-backup`.

Once I have backed up the raw data, I use PySpark to process it. The code to spark scripts can be found [here](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/tree/main/code/scripts) According to the data collected (daily/weekly/monthly), I then run the spark script that parses the data, cleans it, and stores it in an easy-to-use format. When using Airflow, this will be triggered after raw data is backed up.

### Airflow Directed Acyclic Graphs (DAG) 🧑🏻‍🔧
Airflow DAGs are the choreographers of our data dance! 🕺💃

These Directed Acyclic Graphs (DAGs)🕸️ are like the master conductors, guiding the flow of our data tasks. They define the order and dependencies, ensuring each step pirouettes gracefully into the next. Whether it's orchestrating data backups, triggering PySpark scripts, or managing the entire data symphony, Airflow DAGs make it happen.

### Back Up 🦺
After the PySpark magic wraps up, the cleaned data finds its cozy spot in the `steam-project-processed-data` folder (for local runs) or gets whisked away to the AWS S3 Bucket 'steam-clean-storage'. But hey, we're all about that backup life! 🧼✨

Cue the backup script—it ensures the cleaned data has a secure twin at the `steam-project-processed-data-backup` or the `steam-peojecr-raw-backup` S3 bucket. Because, let's face it, backups are the unsung heroes of data life! 🚀(Backups are important 😄) 

### House Keeping♻️
With data safely tucked into its backup haven, it's time for a bit of digital tidying! 🧹 

The pipeline housekeeping job steps in, gracefully clearing out the primary folders. But fear not, our data superheroes stay intact in the backup folders, ready to save the day when needed! 🦸‍♂️📦

### What if something breaks? 🫗
In my previous role, I managed a few pipelines and I know stuff breaks. Picture this: It's Friday evening and your PySpark script suddenly decides it wants a day off, maybe due to unexpected changes in the data source or a cosmic hiccup. It's a data world, after all! 🌌

But fear not! Introducing the superheroes of recovery—the Catch-Up DAGs! 🦸‍♀️💨

These mighty DAGs are your safety nets, ready to swoop in and save the day. Triggered manually, they're designed to catch up on any missed tasks, ensuring your data pipeline gets back on track. Each pipeline has its separate DAG and each DAG has separate stages that can help you run each part of the pipeline independently. Catch Up DAG Because even in the face of surprises, I've got your back! 💪✨

## Visualizations 👀
Time to bring the data to life! 🚀📊

Whether it's the local vibes with Tableau or the cloud magic with AWS QuickSight, our visualizations are the grand finale. And guess what? AWS is all automated! When the data universe detects something new in the S3 storage, these visualization wizards kick into action, turning raw data into insights you can feast your eyes on. 🎉

## Local Setup
- Setup Kafka
- Install PySpark `pip install pyspark`
- Setup Airflow

### Local Setup Issues
Setting up locally is an easy way, however you might face some issues. I use a Windows machine and I have used this [video](https://www.youtube.com/watch?v=BwYFuhVhshI) to set up Kafka. However, Airflow does not work natively on Windows 🥲 The easiest workaround is using Docker 🐋. You can refer to the docker image [here](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/blob/main/docker/docker-compose.yaml). This docker .yaml file contains PySpark and Airflow along with some other python libraries. However, to run the complete project you'd need to find an image that has: Kafka + PySpark + Airflow. This makes the docker container too heavy(16GB+ RAM) and would not run on my laptop. So you can implement the project in parts. Having Kafka run locally. This would help you get raw data. On your raw data, you can build a docker image with Airflow and PySpark, transfer the raw data, and run the DAGs to achieve the cleaned data. Then you'd need to transfer the clean data back to your drive and use Tableau to visualize the results.😤 OOF. 

### Setting Up Docker 🐳
Check the awesome [DOCKER-README.md](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/blob/main/DOCKER-README.md) file  

### AWS to the rescue! 🤌🏻
#### EC2 Instance Configuration
- Launched an EC2 instance optimized for Kafka and Spark usage.
- Configured security groups🔐 to manage inbound/outbound traffic, ensuring Kafka and Spark functionality.
- Established an IAM role granting S3 access to the EC2 instance.
#### Kafka Deployment on EC2
- Installed and set up Kafka on the EC2 instance.
- Configured producer-consumer schemas enabling smooth data flow within Kafka.
#### S3 Bucket Creation
- Created dedicated S3 buckets🪣: `steam-project-raw`, `steam-project-processed-data`, `steam-project-raw-backup` and `steam-project-processed-data-backup`
- Implemented stringent bucket policies to ensure secure data storage.
#### Kafka Data Streaming to S3
- Developed scripts facilitating seamless data streaming from various Steam sources into Kafka topics.
- Integrated scripts to efficiently store data into the designated `steam-project-raw-storage` S3 bucket.
#### Apache Spark Configuration
- Installed and configured Apache Spark on the EC2 instance.
- Crafted Spark scripts adept at transforming and processing data according to daily, weekly, and monthly pipelines.
- Successfully loaded processed data into the steam-clean-storage S3 bucket.
#### S3 Triggers and QuickSight Integration
- Configured S3 event triggers to promptly detect and respond to new data arrivals.
- Integrated QuickSight with S3, enabling direct visualization of data stored in the steam-clean-storage bucket for real-time insights.

## Screenshots

### AirFlow DAGS
- All DAGs
![af0](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/d57fdf20-204c-4fe9-8665-27f7b2d2a79c)

- Daily Dag
![af1](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/898adb5b-2967-4c93-ad70-ea6b7bb332ee)

![af2](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/03a99104-63cc-4c08-a1c7-1babdd8b45ab)

- Catch Up Dag
![af3](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/db3a81f5-603a-4e12-af98-36e78f7b6285)

### AWS S3 Buckets
- All S3 Buckets
![aws_0](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/273fa327-8089-4200-8321-0387cda54a8e)

- Processed Data Bucket
![aws1](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/bee44442-a8a0-4939-abbb-fa5b65742469)

- Raw Data Backup Bucket
![aws2](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/f7b00226-e8d0-4c8c-9466-373fb690a276)

### QuickSight Visualizations
![quicksight0](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/c37edc16-34b9-4a7c-928c-de74be79a022)

## Badges
<div align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka" alt="Apache Kafka" width="120" height="30">
  <img src="https://img.shields.io/badge/Apache%20Spark-FDEE21?style=flat-square&logo=apachespark&logoColor=black" alt="Apache Spark" width="120" height="30">
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white" alt="Apache Airflow" width="120" height="30">
  <img src="https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white" alt="AWS" width="120" height="30">
  <img src="https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" width="120" height="30">
</div>

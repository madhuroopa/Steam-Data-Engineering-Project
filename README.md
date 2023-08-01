# Steam Data Engineering Project - Real-time Data Streaming and Analytics
## Overview
This data engineering project aims to gather real-time data from the Steam platform, perform data transformations using Apache Spark, and store the processed data into a database. The project utilizes web scraping with Selenium to extract information about the most played games, top sellers, weekly top sellers, and website-related KPIs like engagement. The data is then streamed using Kafka, with the web scraping scripts acting as producers and the consumers storing the data in a storage system like an S3 bucket. The processed data can be used for analytics and visualization using tools like Tableau or other dashboard tools.

## Project Components
Web Scraping with Selenium: Python scripts have been developed to perform web scraping on the Steam platform, collecting data on most played games, top sellers, weekly top sellers, and other relevant metrics.

## Kafka Integration:
Kafka is used as a real-time streaming platform to ingest the data obtained from web scraping. The web scraping scripts act as producers, pushing the data to Kafka topics, while the consumers read the data and store it in a storage system like an S3 bucket.

## Data Transformation with Apache Spark: 
Apache Spark is employed to process and transform the real-time data. Spark's distributed computing capabilities allow for efficient processing of large volumes of data.

## Database Storage: 
The processed data is stored in a database, providing a persistent and scalable solution for data storage. The choice of the database can vary based on the project's requirements and preferences.

## Real-Time Data Streaming:
The project focuses on real-time data streaming, ensuring that the data obtained from Steam is available for analysis and visualization with minimal delay.

## Orchestration:
The web scraping scripts can be scheduled and automated using any orchestration tool, enabling regular data updates and ensuring a continuous flow of real-time data into the system.

## Analytics and Visualization: 
The processed data can be utilized for various analytics purposes, such as identifying trends, user behavior patterns, and popular game titles. Visualization tools like Tableau can be employed to create interactive and insightful dashboards for better data exploration.


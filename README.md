# Real-time-Weather-Analytics
Real-Time Weather Data Processing and Visualization
This project implements a real-time pipeline for processing and visualizing weather data using Apache Kafka, Apache Spark, MySQL, and Python.

It demonstrates end-to-end capabilities in Big Data tools, data engineering, and data visualization.

Project Overview

The project aims to collect real-time data and analyze the weather.
It involves gathering information such as temperature, humidity, wind speed, wind direction, atmospheric pressure, precipitation, and more.
The system streams real-time weather data into an Apache Kafka topic, processes the data using Spark Structured Streaming, computes daily statistics (e.g., average/max/min temperature, humidity, and pressure), stores the results in a MySQL database, and visualizes the insights using Python. Additionally, the project analyzes patterns such as:
Temperature trends over time.
Wind patterns, including prevailing winds and gusts.
Air quality by processing pollutant measurements (e.g., particulate matter (PM), ozone (O3), nitrogen dioxide (NO2)).
The weather data used for this project is stored in the weather.csv file, located in the root directory.

 Technologies Used

Apache Kafka: Streaming weather data ingestion.

Apache Spark: Structured Streaming for real-time data processing and aggregation.

MySQL: Persistent storage for processed statistics.

Python: For data analysis and visualization.

Matplotlib & Pandas: Charting and handling tabular data.

PyMySQL: Connecting Python to MySQL.

 Features

Real-time Data Ingestion:

Kafka as a data pipeline.

CSV data ingestion.

Processing with Apache Spark:

Computes daily weather statistics:

 Average / Maximum / Minimum Temperature.

 Average Humidity.

 Average Pressure.

Analyzes additional metrics:

 Wind speed and direction patterns.

Atmospheric pressure trends.

Air quality parameters (e.g., PM, O3, NO2).

Storage:

Stores processed data into a MySQL database.

Visualization:

Charts for insights using Python (Matplotlib):

 Box Plots.

Area Plots.

 Histograms.
 
Project Structure

├── producer.py          # Streams weather data into Kafka topic

├── consumer_1.py        # Inserts raw data from Kafka into MySQL

├── consumer_2.py        # Processes and stores aggregated data

├── plotting.py          # Python script for data visualization

├── weather.csv          # Contains the weather data used for analysis

├── zookeeper/           # Zookeeper scripts and configurations

  ├── .\bin\windows\zookeeper-server-start.bat
  
   ├── .\config\zookeeper.properties
   
├── kafka/               # Kafka scripts and configurations

  ├── .\bin\windows\kafka-server-start.bat
  
  ├── .\config\server.properties
  
 Prerequisites
    

Before running the project, ensure you have the following tools installed and configured:

Kafka & Zookeeper: For streaming.

Apache Spark: For real-time processing.

MySQL: As the database.

Python 3.8+: Required for scripts.

Python Libraries:

pandas

matplotlib

pymysql

pyspark

 How to Run

1. Start Kafka & Zookeeper

Start the Zookeeper and Kafka server:

zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties

2. Set Up MySQL
   
Create a database (e.g., bigdata_db).

Use the provided schema to set up the tables.

3. Stream Data to Kafka
   
Run the producer script:

python producer.py

4. Process Data with Spark
 
Run the consumer scripts:

Consumer 1: For raw data ingestion into MySQL.

python consumer_1.py

Consumer 2: For computing daily weather statistics.

python consumer_2.py

Alternatively, use spark-submit:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.4,org.apache.kafka:kafka-clients:2.8.2 spark-structure-streaming.py

5. Visualize Data
   
Run the visualization script:

python plotting.py

6. Stopping Zookeeper
To stop Zookeeper, execute:

.\bin\windows\zookeeper-server-stop.bat .\config\zookeeper.properties

Key Benefits

Seamlessly handles real-time data processing and storage.

Leverages Big Data frameworks for scalable and efficient analytics.

Delivers insights via rich visualizations.

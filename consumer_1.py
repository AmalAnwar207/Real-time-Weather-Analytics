from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
import pymysql



def insert_all_into_phpmyadmin(row):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "bigdata_db"
    username = "root"
    password = ""
    print("Operator:", row.Formatted_Date)
    print("Crashes:", row.Summary)
    print("Crashes:", row.Precip_Type)
    print("Crashes:", row.Temperature_C)
    print("Crashes:", row.Apparent_Temperature_C)
    print("Crashes:", row.Humidity)
    print("Crashes:", row.Wind_Bearing_degrees)
    print("Crashes:", row.Visibility_km)
    print("Crashes:", row.Cloud_Cover)
    print("Crashes:" , row.Pressure_millibars)
    print("Crashes:", row.Daily_Summary)
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
    column0_value = row.Formatted_Date
    column1_value = row.Summary
    column2_value = row.Precip_Type
    column3_value = row.Temperature_C
    column4_value = row.Apparent_Temperature_C
    column5_value = row.Humidity
    column6_value = row.Wind_Bearing_degrees
    column7_value = row.Visibility_km
    column8_value = row.Cloud_Cover
    column9_value = row.Pressure_millibars
    column10_value = row.Daily_Summary

    print(f"Formatted_Date: {column0_value}, Summary: {column1_value}, Precip_Type: {column2_value}, Temperature_C: {column3_value}, Apparent_Temperature_C: {column4_value}, Humidity: {column5_value}, Wind_Bearing_degrees: {column6_value}, Visibility_km: {column7_value}, Cloud_Cover: {column8_value}, Pressure_millibars: {column9_value}, Daily_Summary: {column10_value}")

    # Prepare the SQL query to insert data into the table
    sql_query = f"INSERT INTO weather_info (Formatted_Date,Summary, Precip_Type, Temperature_C,Apparent_Temperature_C, Humidity, Wind_Bearing_degrees, Visibility_km, Cloud_Cover, Pressure_millibars, Daily_Summary ) VALUES ('{column0_value}', '{column1_value}', '{column2_value}', '{column3_value}', '{column4_value}', '{column5_value}', '{column6_value}', '{column7_value}', '{column8_value}', '{column9_value}', '{column10_value}')"
    
    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()


# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer1") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType()\
    .add("Formatted_Date", TimestampType())\
    .add("Summary", StringType())\
    .add("Precip_Type", StringType())\
    .add("Temperature_C", FloatType())\
    .add("Apparent_Temperature_C", FloatType())\
    .add("Humidity", FloatType())\
    .add("Wind_Bearing_degrees", FloatType())\
    .add("Visibility_km", FloatType())\
    .add("Cloud_Cover", FloatType())\
    .add("Pressure_millibars", FloatType())\
    .add("Daily_Summary", StringType())

# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \




sq = df.select("data.Formatted_Date", "data.Summary","data.Precip_Type","data.Temperature_C","data.Apparent_Temperature_C","data.Humidity","data.Wind_Bearing_degrees","data.Visibility_km","data.Cloud_Cover","data.Pressure_millibars","data.Daily_Summary")







query_sq = sq.writeStream \
.outputMode("append") \
.format("console")\
.foreach(insert_all_into_phpmyadmin) \
.start()

# Wait for the query to finish
query_sq.awaitTermination()
query_sq.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
import pymysql





def insert_into_phpmyadmin(row):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "bigdata_db"
    username = "root"
    password = ""
    print("Operator:", row.Formatted_Date)
    print("Crashes:", row.avg_temperature)
    print("Crashes:", row.max_temperature)
    print("Crashes:", row.min_temperature)
    print("Crashes:", row.avg_humidity)
    print("Crashes:", row.avg_Pressure_millibars)
    
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
    column0_value = row.Formatted_Date
    column1_value = row.avg_temperature
    column2_value = row.max_temperature
    column3_value = row.min_temperature
    column4_value = row.avg_humidity
    column5_value = row.avg_Pressure_millibars

    print(f"Formatted_Date: {column0_value}, avg_temperature: {column1_value}, max_temperature: {column2_value}, min_temperature: {column3_value}, avg_humidity: {column4_value}, avg_Pressure_millibars: {column5_value}")

    # Prepare the SQL query to insert data into the table
    sql_query = f"INSERT INTO weather_exp (Formatted_Date,avg_temperature, max_temperature, min_temperature,avg_humidity, avg_Pressure_millibars ) VALUES ('{column0_value}', '{column1_value}', '{column2_value}', '{column3_value}', '{column4_value}', '{column5_value}')"
    
    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
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

# Select specific columns from "data"
df = df.select("data.Formatted_Date", "data.Temperature_C","data.Humidity","data.Pressure_millibars")


 # Calculate daily weather statistics
daily_stats = (
    df.groupBy(col("Formatted_Date"))
    .agg(
        mean("Temperature_C").alias("avg_temperature"),
        max("Temperature_C").alias("max_temperature"),
        min("Temperature_C").alias("min_temperature"),
        mean("Humidity").alias("avg_humidity"),
        mean("Pressure_millibars").alias("avg_Pressure_millibars")
        )
)
daily_stats.printSchema()  # Print the schema to check data types
# Convert the value column to string and display the result



query = daily_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .foreach(insert_into_phpmyadmin) \
    .start()


# Wait for the query to finish
query.awaitTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaProducer") \
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
# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("mode", "PERMISSIVE") \
    .option("path", "F:/final_project/final_project/data") \
    .load()


json_df = streaming_df.select(to_json(struct(streaming_df.columns)).alias("json_data"))

df = streaming_df.select(to_json(struct("*")).alias("value"))

# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .outputMode("update") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "weather") \
    .option("checkpointLocation", "F:/final_project/final_project/checkpoint")\
    .start()

# Wait for the query to finish
query.awaitTermination()

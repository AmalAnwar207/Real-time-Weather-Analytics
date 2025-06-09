.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties

To run consumer and producer:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.4,org.apache.kafka:kafka-clients:2.8.2 spark-structure-streaming.py

 to stop zookeeper
.\bin\windows\zookeeper-server-stop.bat .\config\zookeeper.properties

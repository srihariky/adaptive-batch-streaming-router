import time
import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession

print("1. Testing Kafka Producer...")
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(5):
    producer.send('test-topic', {'id': i, 'msg': f'hello_{i}', 'ts': time.time()})
producer.flush()
print("   -> Successfully sent 5 records to Kafka KRaft!")

print("2. Testing PySpark Kafka Integration...")
spark = SparkSession.builder \
    .appName("SmokeTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("local[2]") \
    .getOrCreate()

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

count = df.count()
print(f"   -> PySpark successfully read {count} records from Kafka KRaft!")
spark.stop()
print("SUCCESS: Day 1 Block 1 Infrastructure is 100% operational!")

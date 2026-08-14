from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark Session with Iceberg and MinIO (AWS S3) integrations
spark = SparkSession.builder \
    .appName("TillStream_Lakehouse_Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hadoop") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("🚀 Spark Session Initialized with Iceberg & MinIO bindings!")

# 1. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Note: In a true production environment with Confluent Schema Registry, 
# you would use the ABRiS library (za.co.absa:abris) to dynamically pull schemas 
# and strip the 5-byte Confluent Magic Byte.
# For this script, we assume the payload is ready to be written to Iceberg.

# 2. Write to Iceberg tables in MinIO
query = kafka_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .option("path", "lakehouse.raw.orders") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/orders") \
    .start()

print("🌊 Streaming data from Kafka directly into Iceberg Tables on MinIO...")
query.awaitTermination()

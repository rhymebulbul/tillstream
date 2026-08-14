from pyspark.sql import SparkSession

print("🚀 Initializing Spark to Query Iceberg Lakehouse...")
spark = SparkSession.builder \
    .appName("TillStream_Lakehouse_Query") \
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

print("🔍 Querying lakehouse.raw.orders...")
try:
    df = spark.read.format("iceberg").load("lakehouse.raw.orders")
    print("✅ Data successfully retrieved from MinIO Iceberg tables!")
    df.show(20, truncate=False)
except Exception as e:
    print(f"❌ Failed to read data: {e}")

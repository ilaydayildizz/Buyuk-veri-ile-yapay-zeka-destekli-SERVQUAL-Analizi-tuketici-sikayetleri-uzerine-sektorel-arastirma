from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("Sikayetvar_Kafka_Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Arşiv verisiyle birebir aynı şema
schema = StructType() \
    .add("baslik", StringType()) \
    .add("link", StringType()) \
    .add("metin", StringType()) \
    .add("timestamp", StringType())

# Kafka'dan 'sikayetler' topic'ini dinle
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sikayetler") \
    .option("startingOffsets", "earliest") \
    .load()

# Veriyi işle
streaming_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# HDFS'e Append (Ekleme) modunda yaz
# Checkpoint sayesinde kaldığı yeri asla unutmaz
query = streaming_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/sikayetvar/raw_data") \
    .option("checkpointLocation", "hdfs://namenode:8020/sikayetvar/checkpoints") \
    .outputMode("append") \
    .start()

print("Boru hattı kuruldu! Kafka -> HDFS akışı başladı...")
query.awaitTermination()

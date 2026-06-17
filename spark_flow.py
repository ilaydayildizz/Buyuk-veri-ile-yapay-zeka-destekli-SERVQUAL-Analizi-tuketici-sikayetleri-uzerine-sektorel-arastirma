from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Spark oturumunu ağ hatalarına ve replikasyon ihtiyaçlarına göre yapılandırıyoruz
spark = SparkSession.builder \
    .appName("SikayetVar_HDFS_Akisi") \
    .config("spark.network.timeout", "600s") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true") \
    .getOrCreate()

# Gereksiz logları gizle
spark.sparkContext.setLogLevel("WARN")

# Gelen Kafka verisinin şeması
schema = StructType([
    StructField("baslik", StringType(), True),
    StructField("metin", StringType(), True),
    StructField("link", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Kafka'dan okuma başlatıyoruz
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9020") \
    .option("subscribe", "sikayetler") \
    .option("startingOffsets", "earliest") \
    .load()

# JSON veriyi parçalara ayırıyoruz
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# HDFS'e yazma fonksiyonu
def write_to_hdfs(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f">>> Paket {batch_id} HDFS'e yazılıyor. Kayıt sayısı: {batch_df.count()}")
        # coalesce(1) ile küçük dosya oluşumunu engelliyoruz
        # replication=2 ayarı ile verinin hem Master hem Worker'a kopyalanmasını zorluyoruz
        batch_df.coalesce(1).write.mode("append") \
            .option("replication", "2") \
            .parquet("hdfs://namenode:8020/sikayetvar/raw_data")
    else:
        print(f">>> Paket {batch_id} boş, Kafka'dan yeni veri bekleniyor...")

# Akışı v6 checkpoint ile temiz bir şekilde başlatıyoruz
query = json_df.writeStream \
    .foreachBatch(write_to_hdfs) \
    .trigger(processingTime='60 seconds') \
    .option("checkpointLocation", "hdfs://namenode:8020/sikayetvar/checkpoints_v7") \
    .start()

print(">>> Spark Akışı Başlatıldı. Veriler Master ve Worker'a yedekleniyor...")

# Akışı canlı tut
query.awaitTermination()

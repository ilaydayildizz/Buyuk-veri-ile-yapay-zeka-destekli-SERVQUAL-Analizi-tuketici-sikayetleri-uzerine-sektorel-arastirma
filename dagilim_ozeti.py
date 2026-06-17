import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Dagilim_Ozeti").getOrCreate()
df = spark.read.parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")
print("\n" + "📊"*20 + "\n📊 SERVQUAL BOYUTLARI GENEL DAĞILIM TABLOSU\n" + "📊"*20)
df.groupBy("servqual_dimension").count().orderBy("count", ascending=False).show(20, truncate=False)
spark.stop()

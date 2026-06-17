#!/usr/bin/env python3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Saf_LLM_Kontrol").getOrCreate()

# Az önce oluşturulan saf LLM tablosunu oku
df = spark.read.parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")

print("\n📊 TOPLAM SATIR SAYISI:")
print(df.count())

print("\n📊 LLM KATEGORİ DAĞILIMI (HATA VAR MI?):")
df.groupBy("servqual_dimension").count().show(truncate=False)

spark.stop()

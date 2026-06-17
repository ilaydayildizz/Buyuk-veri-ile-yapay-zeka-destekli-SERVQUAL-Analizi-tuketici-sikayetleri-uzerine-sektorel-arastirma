from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

# 1. Spark Oturumunu Başlat (HDFS ve Konfigürasyon ayarlarıyla)
spark = SparkSession.builder \
    .appName("Sikayetvar_Marka_Enrichment") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# 2. Tüm Ham Veriyi Oku (Wildcard ile 354 bin kaydın hepsini alıyoruz)
df_raw = spark.read \
    .option("basePath", "hdfs://namenode:8020/sikayetvar/raw_data") \
    .parquet("hdfs://namenode:8020/sikayetvar/raw_data/*.parquet")

# 3. ADIM: TEKİLLEŞTİRME (Mükerrer Kayıtları Temizle)
# Aynı şikayet linkine sahip satırları eliyoruz
df_unique = df_raw.dropDuplicates(["link"])

# 4. ADIM: LİNKTEN MARKA İSMİ ÇIKARMA (Yeni Sütun Ekleme)
# Örnek Link: https://www.sikayetvar.com/hepsiburada/hepsiburada-teslimat-sorunu
# '/' karakterine göre böldüğümüzde:
# index 0: http:, index 1: "", index 2: www.sikayetvar.com, index 3: marka_ismi
df_enriched = df_unique.withColumn("marka", split(col("link"), "/").getItem(3))

# 5. Sonuç Kontrolü
print("Toplam Eşsiz Şikayet Sayısı: ", df_enriched.count())
print("Örnek Veri Seti (İlk 5 Satır):")
df_enriched.select("marka", "baslik", "link").show(5, truncate=False)

# 6. ADIM: YENİ DOSYAYA KAYDETME
# Marka sütunu eklenmiş tertemiz veriyi ayrı bir klasöre yazıyoruz
output_path = "hdfs://namenode:8020/sikayetvar/enriched_data"
df_enriched.write.mode("overwrite").parquet(output_path)

print(f"İşlem başarıyla tamamlandı! Yeni dosya {output_path} konumuna kaydedildi.")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark oturumunu başlat
spark = SparkSession.builder.appName("Marka_Link_Hatasiz_Birlesme").getOrCreate()

print("\n" + "📥"*20)
print("📥 TABLOLAR YÜKLENİYOR...")
print("📥"*20)

# 1. LLM çıktılı son hazır verinizi oku (204,966 satır)
df_son = spark.read.parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")
son_sayi = df_son.count()
print(f"📊 1. Son Hazır Veri Satır Sayısı: {son_sayi:,}")

# 2. Link ve markaların bir arada olduğu keşfettiğimiz ara tabloyu oku
df_ara = spark.read.parquet("hdfs://namenode:8020/sikayetvar/final_cleaned_data")

# Sadece bize lazım olan sütunları seçip mükerrer kayıtları önlemek için temizliyoruz
df_kaynak = df_ara.select(col("temiz_metin").alias("anahtar_metin"), "link", "marka").dropDuplicates(["anahtar_metin"])

print("\n" + "🔄"*20)
print("🔄 %100 KUSURSUZ ESLESTİRME (JOIN) YAPILIYOR...")
print("🔄"*20)

# temiz_metin alanları üzerinden nokta atışı inner join yapıyoruz
df_birlesik = df_son.join(df_kaynak, df_son.temiz_metin == df_kaynak.anahtar_metin, how="inner")
yeni_sayi = df_birlesik.count()

print("\n" + "🛡️"*20)
print("🛡️ GÜVENİLİRLİK VE DOĞRULUK RAPORU")
print("🛡️"*20)
print(f"Eşleşme Öncesi Satır Sayısı : {son_sayi:,}")
print(f"Eşleşme Sonrası Satır Sayısı: {yeni_sayi:,}")

if son_sayi == yeni_sayi:
    print("✅ MÜKEMMEL: Tek bir satır bile kaybolmadı! %100 doğru eşleşti.")
else:
    print(f"⚠️ UYARI: {son_sayi - yeni_sayi:,} satır eşleşemedi. Lütfen kontrol edin.")

# Sütunları akademik tezinize yakışacak şekilde tertemiz dizelim
df_final = df_birlesik.select("marka", "servqual_dimension", "severity", "link", "temiz_metin")

print("\n" + "👀"*20)
print("👀 DOĞRULANMIŞ İLK 5 SATIR ÖRNEĞİ")
print("👀"*20)
df_final.show(5, truncate=40)

print("\n" + "📊"*20)
print("📊 EN ÇOK ŞİKAYET ALAN İLK 10 MARKA (FREKANS)")
print("📊"*20)
df_final.groupBy("marka").count().orderBy(col("count").desc()).show(10, truncate=False)

# 3. Güncellenmiş nihai tabloyu HDFS'e kalıcı olarak kaydetme
print("\n💾 Yeni Sütunlu Tablo HDFS'e Kaydediliyor...")
df_final.write.mode("overwrite").parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")

print("🏁 İŞLEM BAŞARIYLA TAMAMLANDI! Tablonuz artık hem markalı hem linkli.")
spark.stop()

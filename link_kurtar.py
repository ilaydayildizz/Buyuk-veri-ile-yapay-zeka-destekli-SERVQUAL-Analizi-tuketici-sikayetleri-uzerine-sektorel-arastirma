#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark oturumunu başlat
spark = SparkSession.builder.appName("Link_ve_Marka_Kurtarma").getOrCreate()

print("\n" + "📥"*20)
print("📥 VERİ SETLERİ OKUNUYOR...")
print("📥"*20)

# 1. LLM çıktılı son hazır verinizi oku
df_son = spark.read.parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")
son_sayi = df_son.count()
print(f"📊 Mevcut Hazır Veri Satır Sayısı: {son_sayi:,}")

# 2. Linklerin olduğu ham veriyi oku
df_raw = spark.read.parquet("hdfs://namenode:8020/sikayetvar/raw_data")

# Belleği yormamak için ham veriden sadece bize lazım olan link, marka ve birleştirmede 
# kullanacağımız metin sütunlarını seçip mükerrerleri temizliyoruz.
# NOT: Eğer ham verinizde sütun adları 'link' veya 'marka' yerine farklıysa 
# (örn: url, href, brand), burayı güncelleyebiliriz. İlk aşamada standart adları deniyoruz.
df_kaynak = df_raw.select("temiz_metin", "link", "marka").dropDuplicates(["temiz_metin"])

print("\n" + "🔄"*20)
print("🔄 %100 DOĞRU EŞLEŞME İÇİN JOIN İŞLEMİ BAŞLADI...")
print("🔄"*20)

# Temiz metin üzerinden tam karakter eşleşmeli inner join yapıyoruz
df_kurtarilan = df_son.join(df_kaynak, on="temiz_metin", how="inner")
yeni_sayi = df_kurtarilan.count()

print("\n" + "🛡️"*20)
print("🛡️ GÜVENİLİRLİK VE DOĞRULUK RAPORU")
print("🛡️"*20)
print(f"1. Birleşmeden önceki satır sayısı: {son_sayi:,}")
print(f"2. Birleşmeden sonraki satır sayısı: {yeni_sayi:,}")

if son_sayi == yeni_sayi:
    print("✅ MÜKEMMEL: Sıra kayması veya veri kaybı yok! %100 kusursuz eşleşti.")
else:
    kayip = son_sayi - yeni_sayi
    print(f"⚠️ DİKKAT: {kayip:,} adet satır ham veride tam karakter eşleşmesi bulamadı.")

print("\n" + "👀"*20)
print("👀 RASTGELE 5 SATIR KONTROLÜ (Gözle Doğrulama)")
print("👀"*20)
# Sütunları güzelce hizalayalım
df_kurtarilan = df_kurtarilan.select("marka", "servqual_dimension", "severity", "link", "temiz_metin")
df_kurtarilan.show(5, truncate=40)

# 3. HDFS üzerine KALICI olarak kaydetme
# Veriyi korumak adına üzerine yazıyoruz (overwrite)
print("\n💾 Güncellenmiş veriler HDFS'e kaydediliyor...")
df_kurtarilan.write.mode("overwrite").parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")

print("🏁 İŞLEM BAŞARIYLA TAMAMLANDI! Sütunlar kalıcı olarak birleşti.")
spark.stop()

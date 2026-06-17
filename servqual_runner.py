#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# 1. Spark Oturumunu Başlatma
spark = SparkSession.builder \
    .appName("Sikayetvar_Servqual_LM_Studio_Final") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "12g") \
    .getOrCreate()

# 2. HDFS'ten Ana Veriyi Okuma
df = spark.read.parquet("hdfs://namenode:8020/sikayetvar/final_with_severity")

# 3. LM Studio'yu çökertmemek için veriyi 2 paralel kolda işleme
df_llm = df.repartition(2)

# 4. LM Studio OpenAI Chat Completions UDF Fonksiyonu
def pure_llm_analiz_lm_studio(metin):
    if not metin or metin.strip() == "":
        return "Unclassified"
        
    url = "http://192.168.22.3:1234/v1/chat/completions"
    prompt = f"Metni oku ve YALNIZCA şu kategorilerden birini seç: Reliability, Responsiveness, Assurance, Empathy, Tangibles. Tek bir kelime yaz.\n\nMetin: {metin[:500]}\nKategori:"
    
    payload = {
        "model": "meta-llama-3.1-8b-instruct",
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.0,
        "max_tokens": 15
    }
    
    # Bağlantı kesintilerine karşı 3 Kez Yeniden Deneme (Retry) Mekanizması
    for i in range(3):
        try:
            res = requests.post(url, json=payload, timeout=30)
            if res.status_code == 200:
                json_data = res.json()
                
                # OpenAI / LM Studio formatından cevabı güvenli şekilde çıkarma
                result = json_data['choices'][0]['message']['content'].strip().lower()
                
                # Genişletilmiş Kelime Eşleşme Haritası (Türkçe ve İngilizce Yanıtlar İçerir)
                mapping = {
                    "Reliability": ["reliability", "güvenilirlik", "guvenilirlik", "güvenilir"],
                    "Responsiveness": ["responsiveness", "yanıt", "yanit", "cevap", "heveslilik"],
                    "Assurance": ["assurance", "güvence", "guvence", "güven", "guven"],
                    "Empathy": ["empathy", "empati"],
                    "Tangibles": ["tangibles", "somut", "fiziksel"]
                }
                
                for ana_kategori, alternatifler in mapping.items():
                    for alt in alternatifler:
                        if alt in result:
                            return ana_kategori
                            
                return f"Anlasilmayan_Cevap_{result[:20]}"
            return f"API_Hata_Kodu_{res.status_code}"
            
        except Exception as e:
            if i == 2: # 3 deneme de bittiyse hatayı kaydet
                return f"Baglanti_Hatasi_{type(e).__name__}"
            time.sleep(3) # Hata durumunda 3 saniye dinlendir ve tekrar dene

# 5. UDF'i Spark'a Tanımlama
llm_udf = udf(pure_llm_analiz_lm_studio, StringType())

print("🚀 LM Studio tabanlı kurşungeçirmez işlem başlatılıyor...")

# 6. Yeni Kolonu Ekleme ve Hesaplama
final_df = df_llm.withColumn("servqual_dimension", llm_udf(col("temiz_metin")))

# 7. HDFS'e Üzerine Yazma Moduyla (Overwrite) Kaydetme
final_df.write.mode("overwrite").parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")

print("🏁 İşlem başarıyla tamamlandı!")
spark.stop()

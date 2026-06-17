#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Servqual_Grafik").getOrCreate()
df = spark.read.parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")

# Sadece ana 5 SERVQUAL boyutunu filtreleyip alalım (Anlaşılamayanları dışarıda bırakıp temiz görelim)
ana_boyutlar = ["Empathy", "Tangibles", "Responsiveness", "Assurance", "Reliability"]
servqual_data = df.filter(df.servqual_dimension.isin(ana_boyutlar)).groupBy("servqual_dimension").count().toPandas()
servqual_data = servqual_data.sort_values(by='count', ascending=False)

# Grafik tasarımı
plt.figure(figsize=(10, 6))
sns.set_theme(style="whitegrid")

# Yatay bar grafiği
ax = sns.barplot(x='count', y='servqual_dimension', data=servqual_data, palette="Blues_r")

# Çubukların ucuna sayıları yazma
for p in ax.patches:
    width = p.get_width()
    ax.annotate(f'{int(width):,}', (width, p.get_y() + p.get_height() / 2.),
                ha='left', va='center', xytext=(5, 0), textcoords='offset points', fontsize=11, weight='bold')

plt.title('Şikayet Metinlerinin SERVQUAL Boyutlarına Göre Dağılımı', fontsize=13, pad=15, weight='bold')
plt.xlabel('Şikayet Sayısı (Adet)', fontsize=11)
plt.ylabel('SERVQUAL Hizmet Kalitesi Boyutları', fontsize=11)
plt.tight_layout()

plt.savefig('servqual_dagilimi.png', dpi=300)
print("🏁 SERVQUAL grafiği başarıyla 'servqual_dagilimi.png' olarak kaydedildi!")
spark.stop()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

# Spark oturumu
spark = SparkSession.builder.appName("Siddet_Grafik").getOrCreate()
df = spark.read.parquet("hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm")

# Şiddet kolonunu grupla ve Pandas veri çerçevesine dönüştür
siddet_data = df.groupBy("severity").count().toPandas()

# Grafik tasarımı
plt.figure(figsize=(8, 5))
sns.set_theme(style="whitegrid")

# Renk paleti (Akademik formata uygun soft renkler)
renkler = ['#e74c3c', '#f39c12', '#3498db'] # Yüksek (Kırmızı), Orta (Turuncu), Düşük (Mavi) için temsili

# Çubuk grafiği çizimi
ax = sns.barplot(x='severity', y='count', data=siddet_data, order=['yuksek', 'orta', 'dusuk'], palette=renkler)

# Grafiğin üzerine sayısal değerleri yazma
for p in ax.patches:
    ax.annotate(f'{int(p.get_height()):,}', (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='center', xytext=(0, 8), textcoords='offset points', fontsize=11, weight='bold')

plt.title('Tüketici Şikayetlerinin Şiddet Derecelerine Göre Dağılımı', fontsize=13, pad=15, weight='bold')
plt.xlabel('Şiddet Derecesi', fontsize=11)
plt.ylabel('Şikayet Sayısı (Adet)', fontsize=11)
plt.tight_layout()

# Grafiği kaydet (Teze eklemek üzere ana makineye alacağız)
plt.savefig('siddet_dagilimi.png', dpi=300)
print("🏁 Şiddet grafiği başarıyla 'siddet_dagilimi.png' olarak kaydedildi!")
spark.stop()


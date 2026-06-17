#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Grafiklerin arka plan stilini akademik formata uygun ayarla
sns.set_theme(style="whitegrid")

# ==========================================
# GÖRSEL 1: ŞİDDET DERECESİ GRAFİĞİ
# ==========================================
plt.figure(figsize=(7, 4.5))

# Elimizdeki toplam 204,604 satırın şiddet dağılımı (Empathy, Assurance vs. toplamından beslenen gerçek oranlar)
siddet_data = pd.DataFrame({
    'Şiddet Derecesi': ['Yüksek', 'Orta', 'Düşük'],
    'Şikayet Sayısı': [116421, 83145, 5038] # Model çıktı oranlarına göre dağılım
})

renkler_siddet = ['#e74c3c', '#f39c12', '#3498db']
ax1 = sns.barplot(x='Şiddet Derecesi', y='Şikayet Sayısı', data=siddet_data, palette=renkler_siddet)

for p in ax1.patches:
    ax1.annotate(f'{int(p.get_height()):,}', (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='center', xytext=(0, 8), textcoords='offset points', fontsize=10, weight='bold')

plt.title('Tüketici Şikayetlerinin Şiddet Derecelerine Göre Dağılımı', fontsize=12, pad=15, weight='bold')
plt.xlabel('Şiddet Derecesi', fontsize=10)
plt.ylabel('Şikayet Sayısı (Adet)', fontsize=10)
plt.tight_layout()
plt.savefig('siddet_dagilimi.png', dpi=300)
plt.close()
print("🏁 1. Grafik Başarıyla Oluşturuldu: 'siddet_dagilimi.png'")

# ==========================================
# GÖRSEL 2: SERVQUAL BOYUTLARI GRAFİĞİ
# ==========================================
plt.figure(figsize=(9, 5))

# Spark çıktınızdaki birebir net frekans sayıları
servqual_data = pd.DataFrame({
    'SERVQUAL Boyutu': ['Empathy (Empati)', 'Tangibles (Somut Varlıklar)', 'Responsiveness (Yanıt Verilebilirlik)', 'Assurance (Güvence)', 'Reliability (Güvenilirlik)'],
    'Şikayet Sayısı': [136980, 42473, 10850, 9265, 1034]
})

ax2 = sns.barplot(x='Şikayet Sayısı', y='SERVQUAL Boyutu', data=servqual_data, palette="Blues_r")

for p in ax2.patches:
    width = p.get_width()
    ax2.annotate(f'{int(width):,}', (width, p.get_y() + p.get_height() / 2.),
                ha='left', va='center', xytext=(5, 0), textcoords='offset points', fontsize=10, weight='bold')

plt.title('Şikayet Metinlerinin SERVQUAL Boyutlarına Göre Dağılımı', fontsize=12, pad=15, weight='bold')
plt.xlabel('Şikayet Sayısı (Adet)', fontsize=10)
plt.ylabel('SERVQUAL Hizmet Kalitesi Boyutları', fontsize=10)
plt.tight_layout()
plt.savefig('servqual_dagilimi.png', dpi=300)
plt.close()
print("🏁 2. Grafik Başarıyla Oluşturuldu: 'servqual_dagilimi.png'")

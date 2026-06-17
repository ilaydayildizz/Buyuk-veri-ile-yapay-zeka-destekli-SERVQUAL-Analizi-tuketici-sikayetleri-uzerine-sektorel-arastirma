#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =========================
# CONFIG
# =========================

LM_URL = "http://192.168.22.3:1234/v1/chat/completions"
MODEL = "lmstudio"

INPUT_PATH = "hdfs://namenode:8020/sikayetvar/final_cleaned_data"
OUTPUT_PATH = "hdfs://namenode:8020/sikayetvar/final_with_severity"

# =========================
# SPARK INIT
# =========================

spark = SparkSession.builder \
    .appName("HybridSeverityBatchLLM") \
    .getOrCreate()

df = spark.read.parquet(INPUT_PATH)

df = df.filter(col("temiz_metin").isNotNull())


# =========================
# RULE BASED FILTER
# =========================

HIGH_WORDS = [
    "dolandır", "mahkeme", "icra", "savcılık", "avukat",
    "mağdur", "zarar", "şikayetçiyim", "tüketici hakem",
    "hesabım kapatıldı", "paramı vermiyor"
]

LOW_WORDS = [
    "bilgi almak", "nasıl yapabilirim", "öğrenmek istiyorum",
    "yardımcı olur musunuz", "sormak istiyorum"
]


def rule_check(text):
    t = text.lower()

    high = any(w in t for w in HIGH_WORDS)
    low = any(w in t for w in LOW_WORDS)

    if high and not low:
        return "yuksek"

    if low and not high:
        return "dusuk"

    return "llm"


# =========================
# LLM BATCH FUNCTION
# =========================

def call_llm_batch(batch):

    prompt = """
Sen müşteri şikayet analistisin.

Görev:
Her şikayeti değerlendir ve sadece JSON döndür.

Etiketler:
- yuksek
- orta
- dusuk

Format:
{"0":"yuksek","1":"orta","2":"dusuk"}

Şikayetler:
"""

    for i, t in enumerate(batch):
        prompt += f"\n{i}: {t}"

    try:
        r = requests.post(
            LM_URL,
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0
            },
            timeout=120
        )

        content = r.json()["choices"][0]["message"]["content"]

        match = re.search(r"\{.*\}", content, re.DOTALL)

        if not match:
            return ["orta"] * len(batch)

        data = json.loads(match.group())

        return [
            data.get(str(i), "orta")
            for i in range(len(batch))
        ]

    except Exception:
        return ["orta"] * len(batch)


# =========================
# PARTITION PROCESS
# =========================

def process_partition(iterator):

    batch_texts = []
    batch_rows = []
    results = []

    def flush():

        if not batch_texts:
            return []

        preds = call_llm_batch(batch_texts)

        out = []
        for i, row in enumerate(batch_rows):
            out.append((row.temiz_metin, preds[i]))

        batch_texts.clear()
        batch_rows.clear()

        return out


    for row in iterator:

        decision = rule_check(row.temiz_metin)

        if decision != "llm":
            results.append((row.temiz_metin, decision))
        else:
            batch_texts.append(row.temiz_metin)
            batch_rows.append(row)

        if len(batch_texts) >= 50:
            results.extend(flush())

    results.extend(flush())

    return iter(results)


# =========================
# EXECUTION
# =========================

print("🚀 Veri işleniyor...")

rdd = df.rdd.mapPartitions(process_partition)

df_final = rdd.toDF(["temiz_metin", "severity"])

print("📊 Dağılım:")
df_final.groupBy("severity").count().show()

print("💾 Kaydediliyor...")

df_final.write.mode("overwrite").parquet(OUTPUT_PATH)

print("✅ TAMAMLANDI")

spark.stop()

import requests
from bs4 import BeautifulSoup
import time
import os
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# ======================
# AYARLAR VE DOSYALAR
# ======================
BASE_URL = "https://www.sikayetvar.com"
STATE_FILE = "state.txt"
LINK_FILE = "links.txt"
DATA_FILE = "sikayetler_arsiv.jsonl"
KAFKA_SERVER = "192.168.56.11:9092"
TOPIC = "sikayetler"

# Daha geniş User-Agent listesi (Sitenin seni tanımasını zorlaştırır)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
]

cekilen_linkler = set()

# ======================
# KAFKA BAĞLANTI
# ======================
def kafka_baglan():
    while True:
        try:
            print("🔄 Kafka'ya bağlanıyor...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                retries=5,
                request_timeout_ms=10000
            )
            print("✅ Kafka bağlantısı başarılı!")
            return producer
        except Exception as e:
            print(f"❌ Kafka bağlantı hatası: {e}. 5 saniye sonra tekrar denenecek...")
            time.sleep(5)

producer = kafka_baglan()

# ======================
# YARDIMCI FONKSİYONLAR
# ======================
def linkleri_yukle():
    global cekilen_linkler
    if os.path.exists(LINK_FILE):
        with open(LINK_FILE, "r", encoding="utf-8") as f:
            cekilen_linkler = set(f.read().splitlines())

def veriyi_kaydet(veri):
    try:
        future = producer.send(TOPIC, veri)
        future.get(timeout=5)
    except Exception as e:
        print(f"⚠️ Kafka Gönderim Hatası: {e}")

    try:
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(veri, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"⚠️ Dosya Yazma Hatası: {e}")

def link_kaydet(link):
    with open(LINK_FILE, "a", encoding="utf-8") as f:
        f.write(link + "\n")

def state_oku():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            content = f.read().strip()
            return int(content) if content else 1
    return 1

def state_yaz(sayfa):
    with open(STATE_FILE, "w") as f:
        f.write(str(sayfa))

# ======================
# SCRAPER ÇEKİRDEĞİ
# ======================
def sikayet_detay_cek(link, session):
    # Ana döngüdeki session'ı kullanıyoruz (Daha hızlı ve güvenli)
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": BASE_URL # Gelinen yeri belirterek güven artırıyoruz
    }
    try:
        detay = session.get(link, headers=headers, timeout=15)
        soup = BeautifulSoup(detay.text, "html.parser")
        
        selectors = ["div.complaint-detail-description", "div[itemprop='description']", "section.complaint-detail-description"]
        for sel in selectors:
            metin_el = soup.select_one(sel)
            if metin_el:
                return metin_el.get_text(separator=' ', strip=True)
        return "Metin bulunamadı"
    except Exception:
        return "Bağlantı Hatası"

def full_cycle_scrape():
    linkleri_yukle()
    # Session kullanarak TCP bağlantısını açık tutuyoruz (Hız kazandırır)
    session = requests.Session()
    
    while True:
        current_page = state_oku()
        print(f"\n🚀 Tarama başlıyor. Kaldığı sayfa: {current_page}")

        for sayfa_no in range(current_page, 201):
            url = f"{BASE_URL}/sikayetler?page={sayfa_no}"
            
            # Gelişmiş Header (Siteyi gerçek kullanıcı olduğuna ikna eder)
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "tr-TR,tr;q=0.8,en-US;q=0.5,en;q=0.3",
                "Connection": "keep-alive",
                "Referer": "https://www.google.com/"
            }
            
            try:
                print(f"📂 Sayfa {sayfa_no} taranıyor...", end="\r")
                res = session.get(url, headers=headers, timeout=20)
                soup = BeautifulSoup(res.text, "html.parser")
                kartlar = soup.select("article.card-v2")

                # --- 56. SAYFA TAKILMASINI ÇÖZEN KISIM ---
                deneme = 0
                while not kartlar and deneme < 3:
                    deneme += 1
                    # Sayfa boşsa modemi aç-kapa yapmak yerine 15 saniye bekleyip kimlik değiştirir
                    print(f"\n⚠️ Sayfa {sayfa_no} boş! Engel olabilir. {deneme * 15} sn bekleniyor...")
                    time.sleep(deneme * 15)
                    headers["User-Agent"] = random.choice(USER_AGENTS)
                    res = session.get(url, headers=headers, timeout=20)
                    soup = BeautifulSoup(res.text, "html.parser")
                    kartlar = soup.select("article.card-v2")

                if not kartlar:
                    print(f"\n❌ Sayfa {sayfa_no} hala boş. Atlanıyor...")
                    continue

                yeni_bulunan = 0
                for kart in kartlar:
                    baslik_el = kart.select_one("h3.complaint-title a")
                    if not baslik_el: continue

                    link = BASE_URL + baslik_el.get("href")

                    if link not in cekilen_linkler:
                        baslik = baslik_el.get_text(strip=True)
                        metin = sikayet_detay_cek(link, session)

                        veri_paketi = {
                            "baslik": baslik,
                            "metin": metin,
                            "link": link,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }

                        veriyi_kaydet(veri_paketi)
                        cekilen_linkler.add(link)
                        link_kaydet(link)
                        yeni_bulunan += 1
                        # Detay çekimleri arası çok kısa bekleme (Ban riskini azaltır)
                        time.sleep(random.uniform(0.5, 1.2))

                print(f"\n✅ Sayfa {sayfa_no} bitti. ({yeni_bulunan} yeni şikayet)")
                state_yaz(sayfa_no + 1)
                
                # Sayfa geçişleri arası nefes alma
                time.sleep(random.uniform(1, 3))

            except Exception as e:
                print(f"\n❗ Hata oluştu (Sayfa {sayfa_no}): {e}")
                time.sleep(10)

        print("\n🏁 200 sayfa tamamlandı. Başa dönülüyor (1 dakika mola)...")
        state_yaz(1)
        time.sleep(60)

if __name__ == "__main__":
    full_cycle_scrape()

# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Sikayetvar Sektor Analizi - Golden Master Faz 7") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs://namenode:8020/sikayetvar/final_with_servqual_pure_llm/"
df = spark.read.parquet(hdfs_path)

df_clean = df.withColumn("marka_clean", F.lower(F.trim(F.col("marka"))))

# Faz 7: Kusursuzlaştırılmış Nihai Regex Seti
df_with_sector = df_clean.withColumn(
    "sektor",
    F.when(
        F.col("marka_clean").rlike(
            "trendyol|hepsiburada|amazon|n11|pazarama|ciceksepeti|getir|yemeksepeti|gardrops|glocalzone|dolap|istegelsin|tıkla|tikla|e-ticaret|pazaryeri|avm|pazar|bazaar|market|gross|hipermarket|supermarket|etsy|firsat|taze-cicek|tazecicek|bitiyor|alisveris|kapinda|sosyopix|depo|siparis|topup|lisans|buldum"
        ),
        "E-Ticaret, Pazaryeri & Büyük Marketler"
    )
    .when(
        F.col("marka_clean").rlike(
            "turkcell|vodafone|turk telekom|bimil|giganet|superonline|turknet|millenicom|dsmart|d-smart|digiturk|tivibu|netgsm|comnet|türknet|telekom|gsm|internet|altyapi|fiber|net|elogo|google-ads|bimcell|wifi|vpn|verifykit"
        ),
        "Telekomünikasyon, İnternet & Dijital Servisler"
    )
    .when(
        F.col("marka_clean").rlike(
            "metro-istanbul|motas|motaş|havalimani|havalimanı|ego|iett|ulaşım|ulasim|otobüs|otobus|tren|tcdd|bilet|uçak|ucak|yolculuk|sefer|turizm|tur|kamil-koc|koc|seyahat|hava-yollari|emirates|star-diyarbakir|trabzon-havalimani|oz-elbistan|tour|bus|scooter|beam|koop|itimat|airways|roro|kuralkan|luks-|marti"
        ),
        "Ulaşım & Toplu Taşıma"
    )
    .when(
        F.col("marka_clean").rlike(
            "ziraat|vakifbank|halkbank|garanti|akbank|yapi kredi|is bankasi|qnb|finansbank|ing|teb|papara|tosla|fastpay|pep|fiba|albaraka|kuveyt|katilim|tombank|hadi|hepsifinans|iyi-finans|finans-yatirim|bank|banka|kredi|kart|axess|bonus|maximum|world|yatirim|borsa|halka-arz|forex|crypto|kripto|binance|paribu|tasarruf|sandigi|collecturk|fups|oldubil|paytr|bitay|enpara|teklifim|payeer|cek-kazan|coino|pay"
        ),
        "Finans, Bankacılık & Dijital Ödeme"
    )
    .when(
        F.col("marka_clean").rlike(
            "aras kargo|yurtiçi|yurtici|mng|sürat|surat kargo|ptt kargo|hepsijet|trendyol express|kolay gelsin|sendeo|jetizz|kurye|dağıtım|daagitim|lojistik|kargo|teslimat|nakliye|tasima|dhl|cma-cgm"
        ),
        "Kargo & Lojistik"
    )
    .when(
        F.col("marka_clean").rlike(
            "bet|bahis|casino|iddaa|bilyoner|nesine|misli|tuttur|oleyley|ganyan|loto|lottery|poker|slot|bahsine|zlot|stakecom|oleycom"
        ),
        "Şans Oyunları & Bahis"
    )
    .when(
        F.col("marka_clean").rlike(
            "beymen|skechers|fashfed|occasion|straswans|scarf|wears|giyim|butik|tekstil|ayakkabi|ayakkabı|kıyafet|kiyafet|prive|concept|store|tasarim|tasarım|saraciye|leather|tesbih|nike|sal|collection|wear|no362|lovesneakertr|selin-shoes|rabel-collection|boaz|pantolon|gomlek|gömlek|elbise|mont|ceket|terzi|stil|forma|ipek|kuyumcu|atasay|jewel|pırlanta|pirlanta|saat|kravat|modamerve|damat|gant|laminta|vatkali|kifidis|design|family|greyder|spor|moda|hammer-jack|madmext|manc|bag|taki|takı|luxury|north-face|osse|slazenger|jiber|shefoon|gizce|oakley|ykm|michael-kors|fila|shoes|sneakers|bilezik"
        ),
        "Giyim, Gözlük & Mücevherat"
    )
    .when(
        F.col("marka_clean").rlike(
            "kebap|pizza|cikolata|çikolata|lunchbox|marmaris|salgam|şalgam|su|peynir|gıda|gida|döner|doner|burger|kahve|restaurant|cafe|lokanta|pasta|unlu mamul|firin|fırın|kuruyemis|kuruyemiş|sekerleme|şekerleme|fersan|hasata|menemenci|konak-sekerleme|anadolu-ciftligi|catering|yemek|mutfak|tatli|helva|sut|süt|et|erpilic|erpilice|knorr|dondurma|ciftli|çiftli|usta|ogullari|oğulları|midpoint|yedigun|altinkilic|seker|şeker|simit-|eker|cebel|nilky|kardag|muratbey|bazlama|cemo|arby|toybox|drink"
        ),
        "Yiyecek, İçecek & Restoran"
    )
    .when(
        F.col("marka_clean").rlike(
            "games|steam|ome-tv|tv|playstation|xbox|nintendo|riot|valorant|pubg|epiclol|blizzard|twitch|kick|discord|spotify|netflix|gain|exxen|blutv|tinder|passo|arena|masomo|itempazar|oyun|sinema|biletix|konser|tiyatro|eglence|fenerbahce|kulubu|iticket|bisan|muud|township|beachpark|pluscom|online"
        ),
        "Eğlence, Spor & Dijital İçerik"
    )
    .when(
        F.col("marka_clean").rlike(
            "mobilya|dekorasyon|koltuk|perde|avize|yatak|pasabahce|paşabahçe|jotun|divanev|storish|moms-brand|roomart|jotun-boya|sandalye|masa|hali|halı|çeyiz|ceyiz|mutfak-esyalari|furniture|konfor|depo-dunya|bernardo|floorpan|teska|variodor|garden|artema|formeya"
        ),
        "Mobilya & Ev Dekorasyon"
    )
    .when(
        F.col("marka_clean").rlike(
            "sinbo|home|baseus|gamepower|amstrad|zippo|ev_aletleri|mutfak|beyaz esya|beyaz eşya|ankastre|aprilla|kombi|klima|isitma|soba|supurge|süpürge|buzdolabi|çamaşır|bulasik|bulaşık|philips|fissler|electric|airfel|sogutma|yasomi|vanish|kosla|enplus|regal|parex|awox|franke|miele"
        ),
        "Beyaz Eşya & Ev Elektroniği"
    )
    .when(
        F.col("marka_clean").rlike(
            "site-yonetimi|yonetim|yönetim|apartman|rezidans|konut|site_yonetim|emlakjet|emlak|gayrimenkul|insaatt|arsa|konut|kira|sinpas|remax|apsiyon"
        ),
        "Gayrimenkul & Site Yönetimi"
    )
    .when(
        F.col("marka_clean").rlike(
            "hastane|klinik|medikal|adsm|adsh|saglik|sağlık|dental|doktor|eczane|optik|tıp|tip-merkezi|fizik tedavi|psikolog|veteriner|laboratuvar|clinic|remini|ducray|optima|cihaz|comedones|yesilyurt-agiz|liva-clinic|rosso-beauty|zeynep-cam-guzellik|kozmetik|parfum|parfüm|makeup|guzellik|hair|sac|saç|shampoo|sampuan|estetik|lens|bioderma|hipp|lansinoh|velavit|amare|bebek|baby|yves|rocher|beauty|vitamin|fitamine|wiwify|enterogermina|ezmeci|neutrogena|she-vec|omorfia|dr-|prof-|hospital|bioxcin|peros|fizyostation|meditopia|gratis|dalin|solgar|minies|kids"
        ),
        "Sağlık, Kozmetik & Anne-Bebek"
    )
    .when(
        F.col("marka_clean").rlike(
            "kolej|okul|akademi|kurs|universite|üniversite|egitim|eğitim|dershane|yayin|yayın|kitap|kırtasiye|kirtasiye|kampüs|kampus|novakid|preply|dil_okulu|kreş|kres|halkkitabevi|scribd|kultur|kültür|muze|müze|cambly|kariyer|sorbil|tusdata|d-r|evvel-cevap|kitabevi"
        ),
        "Eğitim & Kültür"
    )
    .when(
        F.col("marka_clean").rlike(
            "teknoloji|bilisim|yazilim|software|tech|bilişim|elektronik|bilgisayar|bilişim|fintables|dijipol|cagri-merkezi|hosting|bulut|xiaomi|outlook|hotmail|microsoft|tenorshare|alcatel|nextstar|teknofinal|mobiclub|cvwizard|artphonely|telefon|cihaz|parca|parça|monofe|xenon|smart|omix|iphone|tekno|bood|epson|mobile|preo|ugreen|electroll|ezglobal|torima"
        ),
        "Teknoloji & Yazılım"
    )
    .when(
        F.col("marka_clean").rlike(
            "sigorta|faktoring|emeklilik|dijipol|kasko|police|poliçe|aksigorta|allianz|anadolu-sigorta"
        ),
        "Sigorta & Emeklilik"
    )
    .when(
        F.col("marka_clean").rlike(
            "belediye|baskanligi|başkanlığı|kaymakamlik|kaymakamlık|mudurlugu|müdürlüğü|valilik|bakanli|bakanlığı|iskur|işkur|sgk|icra|bddk|e-devlet|nüfus|nufus|adliye|kart|dernegi|derneği|deniz-feneri|vakif|vakıf|dernek|turmob|polis-evi|diyanet|afad"
        ),
        "Kamu Kurumları & STK"
    )
    .when(
        F.col("marka_clean").rlike(
            "otomotiv|auto|car|motor|oto|borusan|audi|bmw|mercedes|renault|fiat|hyundai|toyota|volkswagen|ford|honda|lastik|servis|tamir|yaman-teknik|summit-teknik|teknik-servis|arac-kiralama|rent-a-car|aku|akü|falcon|chery|avis|arabam|togg|trumore|jaguar|kompozit|peugeot|citroen|opel"
        ),
        "Otomotiv & Araç Kiralama"
    )
    .when(
        F.col("marka_clean").rlike(
            "otel|tatil|pansiyon|acente|vize|idata|rental|termal|viagogo|radisson|konaklama|resort|dinlenme|tesis|nevmekan|vfs|stayforlong|palace|zorlu-center|muzayede"
        ),
        "Turizm & Konaklama"
    )
    .when(
        F.col("marka_clean").rlike(
            "mağaza|magaza|shop|kedi|pet|mama|hediyenkart|vitamanya|aktar|dogal-file|hunnap|heryerbitki|potikare|oyuncak|tobacco"
        ),
        "Perakende Mağazacılık (Diğer)"
    )
    .when(
        F.col("marka_clean").rlike(
            "instagram|facebook|twitter|tiktok|telegram|whatsapp|dolandirici|numara-kime|sosyal|medya|linkedin"
        ),
        "Sosyal Medya & İnternet Başlıkları"
    )
    .when(
        F.col("marka_clean").rlike(
            "ajans|reklam|medya|danismanlik|danışmanlık|hukuk|avukat|temizlik|guvenlik|güvenlik|lojistik|kurye|av-"
        ),
        "Kurumsal Hizmetler & Danışmanlık"
    )
    .when(
        F.col("marka_clean").rlike(
            "elektrik|enerji|gaz|dağıtım|dagitim|enerjisa|cedas|gediz|ck-|aksa|as"
        ),
        "Enerji, Elektrik & Altyapı Hizmetleri"
    )
    .when(
        F.col("marka_clean").rlike(
            "holding|insaat|inşaat|sanayi|fabrika|cimento|çimento|boru|celik|çelik|metal|plastik|makine|makina|endustri|endüstri|kimya|petrol|rams-turkiye|yapi|evac|fix|huglu"
        ),
        "Sanayi, Üretim & İnşaat"
    )
    .otherwise("Diğer / Sektör Belirlenemedi"),
)

# Nihai Veriyi HDFS'e Kaydetme Adımı (Analiz bittiği için üzerine yazıyoruz veya yeni çıktı üretiyoruz)
# df_with_sector.write.mode("overwrite").parquet("hdfs://namenode:8020/sikayetvar/final_sectored_data/")

# Raporlama
print("=" * 75)
print("📊 AGRESİF ERİTME - FAZ 7 (NİHAİ ALTIN VURUŞ) SONUÇLARI")
print("=" * 75)

sector_report = (
    df_with_sector.groupBy("sektor")
    .agg(
        F.countDistinct("marka").alias("benzersiz_marka_sayisi"),
        F.count("*").alias("toplam_sikayet_sayisi")
    )
    .orderBy(F.desc("toplam_sikayet_sayisi"))
)

sector_report.show(50, truncate=False)

# Raporlama bittikten sonra veriyi HDFS üzerinde kalıcı bir katmana yazıyoruz
output_hdfs_path = "hdfs://namenode:8020/sikayetvar/processed/golden_sectored_data"

print(f"\n💾 Sektör etiketli nihai veri seti HDFS'e kaydediliyor: {output_hdfs_path}")

df_with_sector.write \
    .mode("overwrite") \
    .parquet(output_hdfs_path)

print("✅ Kayıt işlemi başarıyla tamamlandı! Data Pipeline üretime hazır.")

spark.stop()

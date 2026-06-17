from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'sikayet', # Topic adın
    bootstrap_servers=['localhost:9092'], # Burası 9092 kalmalı
    api_version=(2, 8, 1), # Loglarında gördüğümüz versiyon (2.8.1)
    auto_offset_reset='earliest'
)

print("Bağlantı başarılı! Mesajlar bekleniyor...")
for message in consumer:
    print(f"Mesaj alındı: {message.value.decode('utf-8')}")

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'stock-stream',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='stock-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("📥 Consuming stock data from Kafka...")

for message in consumer:
    data = message.value
    print(f"Consumed: {data}")

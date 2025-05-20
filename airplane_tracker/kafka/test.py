from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'flights',  # updated from 'aircraft_live'
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-group'
)

print("✅ Listening to Kafka topic `flights`...")
for msg in consumer:
    print("📦 Received:", msg.value)
    break
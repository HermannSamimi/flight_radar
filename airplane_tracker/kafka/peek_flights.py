from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'flights',
    bootstrap_servers='localhost:19092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m)
)
for i, msg in enumerate(consumer):
    print(msg.value)
    if i >= 4:  # print up to 5 messages
        break
consumer.close()
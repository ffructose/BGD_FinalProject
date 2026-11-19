import json
from urllib.request import urlopen
from kafka import KafkaConsumer
from jsonschema import validate

TOPICS = ("shop.public.customers", "shop.public.products", "shop.public.orders")
BOOTSTRAP = "localhost:9092"
GROUP_ID = "shop.group"
REGISTRY = "http://localhost:8081"

def main():
    # reading schemas
    schemas = {}
    for topic in TOPICS:
        with urlopen(f"{REGISTRY}/subjects/{topic}-value/versions/latest") as response:
            schemas[topic] = json.loads(json.load(response)["schema"])

    # Kafka consumer script that reads AVRO or JSON messages 
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )

    print("Consuming online shop topics...")
    try:
        for msg in consumer:
            if msg.value is None:
                continue

            # and decodes them.
            validate(msg.value, schemas[msg.topic])
            event = msg.value.get("payload", msg.value)
            print(
                f"[topic={msg.topic} partition={msg.partition} offset={msg.offset}] "
                f"key={msg.key} value={event} (JSON schema valid)"
            )
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

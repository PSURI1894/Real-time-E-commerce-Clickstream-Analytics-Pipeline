import json
import logging
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CollectorProducer")

class ClickstreamProducer:
    def __init__(self):
        conf = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'clickstream-collector',
            # Idempotence and high durability configurations
            'acks': 'all',
            'enable.idempotence': True,
            'max.in.flight.requests.per.connection': 5,
            'retries': 2147483647,
            # High throughput settings
            'linger.ms': 20,
            'compression.type': 'snappy',
            'batch.num.messages': 5000
        }
        try:
            self.producer = Producer(conf)
            self._ready = True
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            self._ready = False

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send(self, topic: str, key: str, value: dict) -> bool:
        if not self._ready:
            return False
        try:
            self.producer.produce(
                topic,
                key=key.encode('utf-8') if key else None,
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            # Trigger delivery callbacks periodically to process queued items
            self.producer.poll(0)
            return True
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
            return False

    def is_ready(self) -> bool:
        return self._ready

    def flush(self):
        if self._ready:
            self.producer.flush()

# Kafka logs configurations

# Performance tuned

# Compression snappy

# Idempotency turned on

# Acks all

# Client timeouts

# Buffer configs

# Thread safety logs

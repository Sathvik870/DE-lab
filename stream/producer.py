"""
==============================================================================
Stream Ingestion: High-Velocity Kafka Event Stream Generator
==============================================================================
Module: stream.producer
Description:
    Generates realistic, high-velocity e-commerce event streams (product views,
    cart additions, purchases, price updates) and publishes them to Kafka.
    Supports controlled anomaly injection for Dead-Letter Queue (DLQ) testing.

Class Comments:
    In modern real-time streaming architectures, event streams exhibit high
    velocity and schema variability. Testing stream integrity requires an
    event generator capable of:
    1. Realistic domain data generation across product categories and brands.
    2. High-throughput bursts and continuous rate-limited emission.
    3. Intentional anomaly injection (corrupted JSON, negative values, nulls)
       to validate downstream PySpark schema validation and DLQ routing.
==============================================================================
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from faker import Faker

fake = Faker()

BRANDS = ["boAt", "Sony", "Noise", "Boult", "JBL", "OnePlus", "Realme", "Apple", "Samsung", "Sennheiser"]
CATEGORIES = [
    "Active Noise Cancellation",
    "True Wireless Stereo",
    "Gaming Low Latency",
    "Sports & Fitness",
    "Audiophile Hi-Res",
]
EVENT_TYPES = ["view", "add_to_cart", "purchase", "price_update"]


class EarbudsEventGenerator:
    """Generates synthetic e-commerce product event payloads."""

    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)

    def generate_valid_event(self) -> Dict[str, Any]:
        """Generate a clean, schema-compliant product event."""
        brand = random.choice(BRANDS)
        category = random.choice(CATEGORIES)
        model_name = f"{brand} {fake.word().capitalize()} {random.choice(['Pro', 'Max', 'Ultra', 'Air', 'ANC', 'Lite'])}"
        
        mrp = round(random.uniform(999.0, 24999.0), 2)
        discount_pct = round(random.uniform(10.0, 75.0), 1)
        selling_price = round(mrp * (1.0 - discount_pct / 100.0), 2)
        rating = round(random.uniform(3.0, 5.0), 1)
        rating_count = random.randint(50, 50000)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": random.choice(EVENT_TYPES),
            "product_id": f"EB-{random.randint(1000, 9999)}",
            "url": f"https://example.com/products/{brand.lower()}-{fake.slug()}",
            "title": model_name,
            "price": selling_price,
            "mrp": mrp,
            "discount": discount_pct,
            "rating": rating,
            "rating_count": rating_count,
            "category": category,
            "brand": brand,
            "description": fake.sentence(nb_words=12),
            "highlights": f"Bluetooth 5.3, {random.randint(20, 60)}h Battery, Fast Charging",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def generate_anomalous_event(self) -> Dict[str, Any]:
        """Generate a poisoned/invalid event designed to trigger DLQ quarantine."""
        event = self.generate_valid_event()
        anomaly_type = random.choice([
            "negative_price",
            "missing_title",
            "invalid_rating_high",
            "invalid_discount_over_100",
            "empty_product_id",
            "unrealistic_mrp",
        ])

        if anomaly_type == "negative_price":
            event["price"] = -500.0
            event["anomaly_note"] = "Negative price injected"
        elif anomaly_type == "missing_title":
            event["title"] = ""
            event["anomaly_note"] = "Blank title injected"
        elif anomaly_type == "invalid_rating_high":
            event["rating"] = 9.8  # Exceeds 5.0 star maximum
            event["anomaly_note"] = "Out of range rating (>5.0)"
        elif anomaly_type == "invalid_discount_over_100":
            event["discount"] = 150.0  # Impossible discount percentage
            event["anomaly_note"] = "Discount exceeds 100%"
        elif anomaly_type == "empty_product_id":
            event["product_id"] = None
            event["anomaly_note"] = "Null primary key product_id"
        elif anomaly_type == "unrealistic_mrp":
            event["mrp"] = -100.0
            event["anomaly_note"] = "Negative MRP"

        return event

    def generate_event(self, inject_anomaly: bool = False) -> Dict[str, Any]:
        """Convenience alias to generate either valid or anomalous events."""
        if inject_anomaly:
            return self.generate_anomalous_event()
        return self.generate_valid_event()

    def generate_event_json(self, inject_anomaly: bool = False) -> str:
        """Serialize generated event to a JSON string."""
        return json.dumps(self.generate_event(inject_anomaly=inject_anomaly))

    def generate_batch(self, count: int = 10, anomaly_rate: float = 0.15) -> List[Dict[str, Any]]:
        """
        Generate a batch of streaming events with a controlled ratio of anomalies.

        Args:
            count: Number of events to generate.
            anomaly_rate: Proportion of events that contain validation anomalies.

        Returns:
            List of event dictionaries.
        """
        events = []
        for _ in range(count):
            if random.random() < anomaly_rate:
                events.append(self.generate_anomalous_event())
            else:
                events.append(self.generate_valid_event())
        return events


class KafkaStreamProducer:
    """Publishes streaming product events to an Apache Kafka broker."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "earbuds_stream"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.generator = EarbudsEventGenerator()
        self._producer = None

    def _get_producer(self):
        """Lazy initialization of KafkaProducer."""
        if self._producer is None:
            try:
                from kafka import KafkaProducer

                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                )
                print(f"[KAFKA PRODUCER] Connected to broker: {self.bootstrap_servers}")
            except Exception as e:
                print(f"[KAFKA PRODUCER] Failed to connect to Kafka broker: {e}")
                raise
        return self._producer

    def publish_event(self, event: Dict[str, Any]) -> None:
        """Publish a single event to the Kafka topic."""
        producer = self._get_producer()
        key = str(event.get("product_id") or "unassigned")
        producer.send(self.topic, key=key, value=event)

    def publish_events(
        self,
        count: int = 50,
        rate_per_sec: float = 10.0,
        anomaly_rate: float = 0.15,
        continuous: bool = False,
    ) -> int:
        """
        Stream events into Kafka topic at a controlled throughput.

        Args:
            count: Total events if not continuous.
            rate_per_sec: Throughput velocity in events/second.
            anomaly_rate: Fraction of events that violate schema rules.
            continuous: Run indefinitely until interrupted.

        Returns:
            Total published event count.
        """
        delay = 1.0 / max(rate_per_sec, 0.1)
        published = 0

        print("=" * 60)
        print(f"[KAFKA STREAM] Publishing to topic '{self.topic}' at {rate_per_sec} events/sec...")
        print(f"[KAFKA STREAM] Anomaly Injection Rate: {anomaly_rate * 100:.1f}%")
        print("=" * 60)

        try:
            while continuous or published < count:
                is_anomaly = random.random() < anomaly_rate
                event = self.generator.generate_anomalous_event() if is_anomaly else self.generator.generate_valid_event()
                
                self.publish_event(event)
                published += 1

                if published % 10 == 0:
                    status_flag = "[ANOMALY]" if is_anomaly else "[VALID]"
                    print(f"[KAFKA STREAM] {published} events sent. Latest {status_flag}: {event.get('title') or 'Null'}")

                time.sleep(delay)

            self._producer.flush()
            print(f"[KAFKA STREAM] Successfully published {published} events.")
            return published

        except KeyboardInterrupt:
            print(f"\n[KAFKA STREAM] Stream generator stopped by user. Total events sent: {published}")
            if self._producer:
                self._producer.flush()
            return published

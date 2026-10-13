"""
==============================================================================
Unit & Integration Tests: Stream Producer & Anomaly Generator
==============================================================================
Module: tests.test_stream_producer
Description:
    Validates synthetic event generation, schema compliance, and controlled
    anomaly injection for Kafka streaming and Dead-Letter Queue (DLQ) testing.
==============================================================================
"""

import json
from stream.producer import EarbudsEventGenerator


class TestStreamProducer:
    """Tests for simulated high-velocity event generator."""

    def setup_method(self):
        self.generator = EarbudsEventGenerator(seed=42)

    def test_generate_clean_event_schema(self):
        """Verify generated clean events satisfy expected schema and value types."""
        event = self.generator.generate_event(inject_anomaly=False)

        assert isinstance(event, dict)
        assert "event_id" in event
        assert "product_id" in event
        assert "title" in event
        assert "brand" in event
        assert "category" in event
        assert "price" in event
        assert "mrp" in event
        assert "discount" in event
        assert "rating" in event
        assert "rating_count" in event
        assert "timestamp" in event

        # Value constraints for clean events
        assert event["product_id"].startswith("EB-")
        assert len(event["title"]) > 0
        assert event["price"] > 0
        assert event["mrp"] >= event["price"]
        assert 0.0 <= event["rating"] <= 5.0
        assert event["rating_count"] >= 0

    def test_generate_event_json_serialization(self):
        """Verify events can be serialized to JSON and parsed without errors."""
        json_str = self.generator.generate_event_json(inject_anomaly=False)
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed["product_id"].startswith("EB-")

    def test_anomaly_injection_produces_violations(self):
        """Verify that enabling anomaly injection introduces detectable schema violations."""
        anomaly_types_found = set()

        for _ in range(50):
            event = self.generator.generate_event(inject_anomaly=True)
            if event.get("price", 1) < 0:
                anomaly_types_found.add("negative_price")
            if event.get("title") == "":
                anomaly_types_found.add("missing_title")
            if event.get("rating", 2.5) > 5.0 or event.get("rating", 2.5) < 0:
                anomaly_types_found.add("invalid_rating")
            if event.get("discount", 0) > 100.0:
                anomaly_types_found.add("invalid_discount")
            if event.get("product_id") is None:
                anomaly_types_found.add("empty_product_id")

        # Over 50 iterations, at least two distinct anomaly classes should be produced
        assert len(anomaly_types_found) >= 2

    def test_batch_generation_count(self):
        """Verify generate_batch produces the requested event count."""
        batch = self.generator.generate_batch(count=25, anomaly_rate=0.0)
        assert len(batch) == 25
        assert all(isinstance(x, dict) for x in batch)

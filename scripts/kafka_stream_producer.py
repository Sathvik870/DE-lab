#!/usr/bin/env python3
"""
==============================================================================
CLI Tool: High-Velocity Kafka Event Stream Producer
==============================================================================
Description:
    Command-line interface to simulate high-velocity e-commerce streams into Kafka.

Usage:
    python -m scripts.kafka_stream_producer --events 100 --rate 20
    python -m scripts.kafka_stream_producer --continuous --rate 10 --anomaly-rate 0.2
    python -m scripts.kafka_stream_producer --events 5 --dry-run
==============================================================================
"""

import argparse
import json
import sys
import os

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from stream.producer import EarbudsEventGenerator, KafkaStreamProducer
from etl.config import kafka_config


def main():
    parser = argparse.ArgumentParser(
        description="High-Velocity Simulated Kafka Event Stream Producer for Earbuds Lakehouse"
    )
    parser.add_argument(
        "--events",
        type=int,
        default=50,
        help="Number of events to generate (default: 50)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Throughput velocity in events/second (default: 10.0)",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.15,
        help="Fraction of events containing validation anomalies (default: 0.15 / 15%%)",
    )
    parser.add_argument(
        "--inject-anomalies",
        action="store_true",
        help="Enable anomaly injection flag (uses default or specified --anomaly-rate)",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously until interrupted (Ctrl+C)",
    )
    parser.add_argument(
        "--server",
        type=str,
        default=kafka_config.server,
        help=f"Kafka bootstrap server (default: {kafka_config.server})",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=kafka_config.stream_topic,
        help=f"Target Kafka topic (default: {kafka_config.stream_topic})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and print events to stdout without connecting to Kafka",
    )

    args = parser.parse_args()

    if args.dry_run:
        print("=" * 60)
        print(f"[DRY RUN] Generating {args.events} simulated events (Anomaly Rate: {args.anomaly_rate * 100}%)...")
        print("=" * 60)
        gen = EarbudsEventGenerator()
        events = gen.generate_batch(count=args.events, anomaly_rate=args.anomaly_rate)
        for idx, event in enumerate(events, start=1):
            flag = "[ANOMALY]" if "anomaly_note" in event else "[VALID]"
            print(f"Event #{idx} {flag}:\n{json.dumps(event, indent=2)}\n")
        print(f"[DRY RUN] Generated {len(events)} events successfully.")
        return 0

    producer = KafkaStreamProducer(bootstrap_servers=args.server, topic=args.topic)
    total_sent = producer.publish_events(
        count=args.events,
        rate_per_sec=args.rate,
        anomaly_rate=args.anomaly_rate,
        continuous=args.continuous,
    )
    return 0 if total_sent > 0 else 1


if __name__ == "__main__":
    sys.exit(main())

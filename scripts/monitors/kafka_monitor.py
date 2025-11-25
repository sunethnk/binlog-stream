#!/usr/bin/env python3
"""
Kafka ALL TOPICS Monitor (Hardcoded Bootstrap Server)

Modify the variables BOOTSTRAP_SERVERS, GROUP_ID, EXCLUDE_TOPICS as you need.
"""

import time
from kafka import KafkaConsumer, TopicPartition

# ---------------------------------------------------------------------
# HARD-CODED SETTINGS — EDIT THESE
# ---------------------------------------------------------------------
BOOTSTRAP_SERVERS = ["localhost:9092"]      # <--- CHANGE YOUR BROKER HERE
GROUP_ID = "my_monitor_group"               # optional, set to None to disable lag
EXCLUDE_TOPICS = ["__consumer_offsets"]     # list of topics to skip
INTERVAL = 5.0                               # seconds
CLIENT_ID = "kafka-monitor-all"
# ---------------------------------------------------------------------


def main():

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=CLIENT_ID,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        api_version_auto_timeout_ms=5000,
    )

    print("Kafka ALL TOPICS Monitor")
    print("---------------------------------------------")
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"Consumer group   : {GROUP_ID}")
    print(f"Exclude topics   : {EXCLUDE_TOPICS}")
    print(f"Interval         : {INTERVAL}s")
    print("---------------------------------------------\n")

    last_end_offsets = {}

    try:
        while True:
            consumer.poll(timeout_ms=200)

            all_topics = sorted(list(consumer.topics()))
            topics = [t for t in all_topics if t not in EXCLUDE_TOPICS]

            if not topics:
                print("No topics found. Retrying...")
                time.sleep(INTERVAL)
                continue

            partitions = []
            for topic in topics:
                parts = consumer.partitions_for_topic(topic)
                if not parts:
                    continue
                for p in parts:
                    partitions.append(TopicPartition(topic, p))

            if not partitions:
                print("No partitions found. Retrying...")
                time.sleep(INTERVAL)
                continue

            end_offsets = consumer.end_offsets(partitions)

            committed_offsets = {}
            if GROUP_ID:
                for tp in partitions:
                    committed_offsets[tp] = consumer.committed(tp)

            now = time.time()

            print("=" * 100)
            print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now))}")
            print("=" * 100)

            header = "{:<35} {:>12} {:>15} {:>15} {:>10}".format(
                "Topic-Partition", "EndOff", "Committed", "Lag", "Rate/s"
            )
            print(header)
            print("-" * 100)

            total_rate = 0.0
            total_lag = 0

            for tp, end in sorted(end_offsets.items(),
                                  key=lambda kv: (kv[0].topic, kv[0].partition)):

                key = (tp.topic, tp.partition)

                committed = committed_offsets.get(tp) if GROUP_ID else None
                lag = max(0, end - committed) if committed is not None else None
                if lag is not None:
                    total_lag += lag

                rate = 0.0
                if key in last_end_offsets:
                    last_off, last_ts = last_end_offsets[key]
                    dt = now - last_ts
                    if dt > 0:
                        rate = (end - last_off) / dt

                last_end_offsets[key] = (end, now)
                total_rate += max(rate, 0.0)

                print(
                    "{:<35} {:>12} {:>15} {:>15} {:>10.2f}".format(
                        f"{tp.topic}[{tp.partition}]",
                        end,
                        committed if committed is not None else "-",
                        lag if lag is not None else "-",
                        rate,
                    )
                )

            print("-" * 100)
            print(f"TOTAL: lag = {total_lag if GROUP_ID else 'N/A'}, "
                  f"total_rate = {total_rate:.2f} msgs/s\n")

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping monitor...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()


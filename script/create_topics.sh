#!/bin/bash
# create_topics.sh

# Wait for Kafka to be ready
sleep 40

# Create topics
kafka-topics.sh --create --topic loan_data_csv --bootstrap-server kafka:9093 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic loan_data_parquet --bootstrap-server kafka:9093 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic loan_data_json --bootstrap-server kafka:9093 --replication-factor 1 --partitions 1

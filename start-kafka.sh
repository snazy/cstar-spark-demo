#!/bin/sh

KAFKA_DIR=/Users/snazy/devel/servers/kafka/kafka_2.11-0.9.1.0-SNAPSHOT

cd $KAFKA_DIR

bin/zookeeper-server-start.sh config/zookeeper.properties > logs/kafka-zk.log &

echo ""
echo "Sleeping 5 seconds..."
echo ""

sleep 5

bin/kafka-server-start.sh config/server.properties > logs/kafka.log &

echo ""
echo "Sleeping 5 seconds..."
echo ""

sleep 5

bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic=cstarSpark --partitions=1 --replication-factor=1

./kafka-topics.sh --create --zookeeper localhost:2181
--replication-factor 1  --partitions 1 --topic HelloWorld

./kafka-topics.sh --list --zookeeper localhost:2181

./kafka-console-producer.sh --broker-list localhost:9092
--topic HelloWrold
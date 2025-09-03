kafka-console-producer.sh --broker-list localhost:9092 --topic test --producer.config ssl.properties
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --consumer.config ssl.properties --from-beginning

docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic flink_topic
docker exec -t -i docker-producer-1 python3 Main.py
docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic flink_topic_2
docker exec -t -i docker-kafka-client-1 python3 ProdMain.py

if [ $1 -eq 1 ] || [ $1 -eq 2 ] || [ $1 -eq 3 ]
then

    ## Kafka topic clearing and creation
    docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic flink_topic_2
    docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic flink_topic_2

    ## Job begin
    docker exec -t -i -d docker-jobmanager-1 /bin/bash /src/query_runner.sh $1 y
    docker exec -t -i -d docker-prometheus-1 prometheus --config.file=./prometheus.yml --web.listen-address=:9010

    ## Production
    docker exec -t -i docker-kafka-client-1 python3 ProdMain.py
else
    echo "ERRORE PARAMETRI"
fi
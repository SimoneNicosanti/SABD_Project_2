
if [ $1 -eq 1 ] || [ $1 -eq 2 ] || [ $1 -eq 3 ]
then
    echo "SETTING UP KAFKA"
    docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic flink_topic
    docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic flink_topic
    docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "Query_*"
    sleep 5

    ## Starting Job
    echo "STARTING JOB"
    docker exec -t -i -d docker-jobmanager-1 /bin/bash /src/query_runner.sh $1 n

    ## Production
    echo "STARTING PRODUCTION"
    docker exec -t -i -d docker-kafka-client-1 python3 ProdMain.py

    ## Consumer
    echo "STARTING CONSUME"
    docker exec -t -i docker-kafka-client-1 python3 ConsMain.py $1
else
    echo "ERRORE PARAMETRI"
fi
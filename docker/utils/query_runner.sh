if [ $1 -eq 1 ] || [ $1 -eq 2 ] || [ $1 -eq 3 ]
then
    echo "INIT JOB"
    docker exec -t -i docker-jobmanager-1 /bin/bash /src/query_runner.sh $1 n

    echo "INIT KAFKA"
    docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic flink_topic
    docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic flink_topic
    sleep 5
    
    ## Job begin
    echo "INIT JOB"
    docker exec -t -i -d docker-jobmanager-1 /bin/bash /src/query_runner.sh $1 y

    ## Production
    echo "STARTING PRODUCTION"
    docker exec -t -i -d docker-kafka-client-1 python3 ProdMain.py
    
else
    echo "ERRORE PARAMETRI"
fi
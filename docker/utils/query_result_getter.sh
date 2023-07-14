
if [ $1 -eq 1 ] || [ $1 -eq 2 ] || [ $1 -eq 3 ]
then
    docker exec -t -i -d docker-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "Query_"$1"_*"
    docker exec -t -i -d docker-jobmanager-1 /bin/bash /src/query_runner.sh $1 n
    docker exec -t -i docker-kafka-client-1 python3 ConsMain.py $1
else
    echo "ERRORE PARAMETRI"
fi
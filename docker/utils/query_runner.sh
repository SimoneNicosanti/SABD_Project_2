if [ $1 -eq 1 ] || [ $1 -eq 2 ] || [ $1 -eq 3 ]
then
    echo "INIT JOB"
    docker exec -t -i docker-jobmanager-1 /bin/bash /src/query_runner.sh $1 y
else
    echo "ERRORE PARAMETRI"
fi
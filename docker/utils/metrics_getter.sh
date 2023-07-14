
if [ $1 -eq 1 ] || [ $1 -eq 2 ] || [ $1 -eq 3 ]
then
    ## Call first clear production and wait for production start, otherwise jobmanager won't find any topic
    docker exec -t -i -d docker-jobmanager-1 /bin/bash /src/query_runner.sh $1 y
    docker exec -t -i docker-prometheus-1 prometheus --config.file=./prometheus.yml --web.listen-address=:9010
else
    echo "ERRORE PARAMETRI"
fi
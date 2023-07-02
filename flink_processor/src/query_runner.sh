

if [ "$#" -eq 1 ] 
then
    echo "Run query $1"
    flink run --jobmanager localhost:8081 --python ./Main.py $1
else
    echo "Errore parametri"
fi
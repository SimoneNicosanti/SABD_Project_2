from engineering import KafkaSingleton
import kafka
import time
import csv

def controller() :

    kafkaProducer = KafkaSingleton.getKafkaProducer()
    kafkaTopic = KafkaSingleton.getKafkaTopic()

    for i in range(0, 10) :
        kafkaProducer.send(
            topic = kafkaTopic,
            value = str(i).encode()
        )

        time.sleep(2)
    
    return
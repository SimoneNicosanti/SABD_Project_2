from kafka import KafkaConsumer
from engineering import KafkaSingleton
from dao import CsvWriter

def consume() :

    kafkaConsumer : KafkaConsumer = KafkaSingleton.getKafkaConsumer()

    print("Waiting for results")
    for msg in kafkaConsumer :
        msgTopic : str = msg.topic
        msgValue : str = msg.value
        CsvWriter.writeCsv(msgTopic, msgValue)
        
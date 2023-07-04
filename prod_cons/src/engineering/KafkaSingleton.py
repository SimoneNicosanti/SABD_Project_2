import kafka
import jproperties
import json

__KAFKA_PRODUCER : kafka.KafkaProducer = None
__KAFKA_TOPIC : str = None
__KAFKA_CONSUMER : kafka.KafkaConsumer = None

def getKafkaProducer() -> kafka.KafkaProducer :
    global __KAFKA_PRODUCER
    
    if (__KAFKA_PRODUCER == None) :
        configs = jproperties.Properties()
        
        kafkaServer = __getKafkaServer()

        __KAFKA_PRODUCER = kafka.KafkaProducer(bootstrap_servers = kafkaServer)
        
    return __KAFKA_PRODUCER

def getKafkaTopic() -> str :
    global __KAFKA_TOPIC
    if (__KAFKA_TOPIC == None) :
        configs = jproperties.Properties()
        with open("./properties/kafka.properties", 'rb') as config_file:
            configs.load(config_file)

            __KAFKA_TOPIC = configs.get("kafka.topic").data
        
    return __KAFKA_TOPIC


def getKafkaConsumer() -> kafka.KafkaConsumer :
    global __KAFKA_CONSUMER
    if (__KAFKA_CONSUMER == None) :
        kafkaServer = __getKafkaServer()
        kafkaConsumer = kafka.KafkaConsumer(
            "Query_1_Hour", "Query_1_Day", "Query_1_Glb",
            bootstrap_servers = kafkaServer,
            auto_offset_reset='earliest'
        )

        __KAFKA_CONSUMER = kafkaConsumer
    
    return kafkaConsumer


def __getKafkaServer() :
    configs = jproperties.Properties()
    with open("./properties/kafka.properties", 'rb') as config_file:
        configs.load(config_file)

        hostName = configs.get("kafka.host").data
        portNumber = configs.get("kafka.port").data

        return str(hostName) + ":" + str(portNumber)
import kafka
import jproperties

__KAFKA_PRODUCER : kafka.KafkaProducer = None
__KAFKA_TOPIC : str = None

def getKafkaProducer() -> kafka.KafkaProducer :
    global __KAFKA_PRODUCER
    
    if (__KAFKA_PRODUCER == None) :
        configs = jproperties.Properties()
        with open("./properties/kafka.properties", 'rb') as config_file:
            configs.load(config_file)

            hostName = configs.get("kafka.host").data
            portNumber = configs.get("kafka.port").data

            __KAFKA_PRODUCER = kafka.KafkaProducer(bootstrap_servers = hostName + ":" + portNumber)
        
    return __KAFKA_PRODUCER

def getKafkaTopic() -> str :
    global __KAFKA_TOPIC
    if (__KAFKA_TOPIC == None) :
        configs = jproperties.Properties()
        with open("./properties/kafka.properties", 'rb') as config_file:
            configs.load(config_file)

            __KAFKA_TOPIC = configs.get("kafka.topic").data
        
    return __KAFKA_TOPIC
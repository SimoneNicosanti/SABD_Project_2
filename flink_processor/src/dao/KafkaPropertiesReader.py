import jproperties

def getKafkaProperties() :
    configs = jproperties.Properties()

    with (open("./properties/kafka.properties", "rb")) as kafka_conf :

        configs.load(kafka_conf)

        kafkaHost = str(configs.get("kafka.host").data)
        kafkaPort = str(configs.get("kafka.port").data)
        kafkaTopic = str(configs.get("kafka.topic").data)

        bootstrapServer = kafkaHost + ":" + kafkaPort

        return {"host" : kafkaHost, "port" : kafkaPort, "inputTopic" : kafkaTopic}
    

def getKafkaUrl() :
    configs = jproperties.Properties()

    with (open("./properties/kafka.properties", "rb")) as kafka_conf :

        configs.load(kafka_conf)

        kafkaHost = str(configs.get("kafka.host").data)
        kafkaPort = str(configs.get("kafka.port").data)

        bootstrapServer = kafkaHost + ":" + kafkaPort

        return bootstrapServer
    

def getKafkaInputTopic() :
    configs = jproperties.Properties()

    with (open("./properties/kafka.properties", "rb")) as kafka_conf :

        configs.load(kafka_conf)

        kafkaTopic = str(configs.get("kafka.topic").data)

        return kafkaTopic

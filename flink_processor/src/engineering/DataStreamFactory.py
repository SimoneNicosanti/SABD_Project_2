from pyflink.common.typeinfo import Types
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from engineering import FlinkEnvFactory
import jproperties


def getDataStream() -> tuple[DataStream, StreamExecutionEnvironment] :
    env = FlinkEnvFactory.getEnv()
    kafkaSource = __getKafkaSource()

    dataStream = env.from_source(kafkaSource, WatermarkStrategy.no_watermarks(), "Kafka Source")

    return (dataStream, env)


def __getKafkaSource() -> KafkaSource :
    
    configs = jproperties.Properties()

    with (open("./properties/kafka.properties", "rb")) as kafka_conf :

        configs.load(kafka_conf)

        kafkaHost = str(configs.get("kafka.host").data)
        kafkaPort = str(configs.get("kafka.port").data)
        kafkaTopic = str(configs.get("kafka.topic").data)

        bootstrapServer = kafkaHost + ":" + kafkaPort

        kafkaSource = KafkaSource.builder(
            ).set_bootstrap_servers(
                bootstrapServer
            ).set_topics(
                kafkaTopic
            ).set_group_id(
                "flink_group"
            ).set_starting_offsets(
                KafkaOffsetsInitializer.earliest()
            ).set_value_only_deserializer(
                SimpleStringSchema()
            ).build()
        
        return kafkaSource
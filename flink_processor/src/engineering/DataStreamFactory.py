from pyflink.common.typeinfo import Types
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment

from engineering import FlinkEnvFactory
import jproperties
import datetime
import json
from dao import KafkaPropertiesReader


def getDataStream() -> tuple[DataStream, StreamExecutionEnvironment] :

    def jsonToTuple(mess : str) :
        jsonObject = json.loads(mess)

        tupleDateTime = datetime.datetime.strptime(jsonObject["TradingDate"] + " " + jsonObject["TradingTime"], "%d-%m-%Y %H:%M:%S.%f")

        return (
                jsonObject["ID"] , 
                jsonObject["SecType"] , 
                float(jsonObject["Last"]) ,
                datetime.datetime.timestamp(tupleDateTime) * 1000
                )
    

    env = FlinkEnvFactory.getEnv()
    kafkaSource = __getKafkaSource()

    dataStream = env.from_source(kafkaSource, WatermarkStrategy.no_watermarks(), "Kafka Source")

    convertedDataStream = dataStream.map( ## (ID, SecType, Last, Timestamp)
            jsonToTuple,
            output_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.FLOAT()])
        )
    
    #convertedDataStream.print()

    return (convertedDataStream, env)



def __getKafkaSource() -> KafkaSource :

    kafkaServer = KafkaPropertiesReader.getKafkaUrl()
    kafkaTopic = KafkaPropertiesReader.getKafkaInputTopic()

    kafkaSource = KafkaSource \
        .builder() \
        .set_bootstrap_servers(kafkaServer) \
        .set_topics(kafkaTopic) \
        .set_group_id("flink_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
        
    return kafkaSource
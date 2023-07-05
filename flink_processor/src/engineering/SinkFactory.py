
from pyflink.common.typeinfo import Types, DateTypeInfo
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee 
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.formats.csv import CsvRowSerializationSchema, CsvBulkWriters, CsvSchema
# from pyflink.common.serialization import

from pyflink.common import SerializationSchema

from dao import KafkaPropertiesReader



def getKafkaSink(topicName : str, serializationSchema : SerializationSchema) -> KafkaSink :
    
    kafkaServer = KafkaPropertiesReader.getKafkaUrl()

    kafkaSink = KafkaSink \
        .builder() \
        .set_bootstrap_servers(kafkaServer) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder() \
                .set_topic(topicName)
                .set_value_serialization_schema(serializationSchema)
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()
    
    return kafkaSink

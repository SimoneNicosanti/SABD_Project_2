from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee 
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.formats.csv import CsvRowSerializationSchema


def getQueryOneSchema() :
    serialization_schema = JsonRowSerializationSchema \
        .builder() \
        .with_type_info(
            type_info=Types.ROW([Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.INT()])
        ) \
        .build()
    
    return serialization_schema
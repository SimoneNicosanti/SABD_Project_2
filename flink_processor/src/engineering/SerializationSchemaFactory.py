from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee 
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.formats.csv import CsvRowSerializationSchema
from pyflink.datastream.formats.csv import CsvRowSerializationSchema, CsvBulkWriters, CsvSchema, CsvSchemaBuilder
from pyflink.table.types import DataTypes

def getQueryOneSchema_JSON() -> JsonRowSerializationSchema:
    serialization_schema = JsonRowSerializationSchema \
        .builder() \
        .with_type_info(
            type_info=Types.ROW([Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.INT()])
        ) \
        .build()
    
    return serialization_schema


def getQueryOneSchema_CSV() -> CsvSchema:
    schema = CsvSchema.builder() \
        .add_number_column('Timestamp', number_type=DataTypes.FLOAT()) \
        .add_string_column('ID') \
        .add_number_column('Avg', number_type=DataTypes.FLOAT()) \
        .add_number_column('Count', number_type=DataTypes.INT()) \
        .set_column_separator(",") \
        .build()
    
    return schema
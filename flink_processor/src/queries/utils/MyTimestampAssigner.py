from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types


class MyTimestampAssigner(TimestampAssigner) :
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        valueTimeStamp = value[1][2]
            
        return valueTimeStamp

from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types


class MyTimestampAssigner(TimestampAssigner) :

    ## On Tuple like (ID, SecType, Last, timestamp)
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        valueTimeStamp = value[3]
            
        return valueTimeStamp

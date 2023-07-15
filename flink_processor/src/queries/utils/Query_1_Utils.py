from pyflink.common.typeinfo import Types
from collections.abc import Iterable

from pyflink.datastream.window import TimeWindow
from pyflink.datastream.functions import ProcessWindowFunction

from pyflink.common.typeinfo import Types

from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowSerializationSchema


class MyProcessWindowFunction(ProcessWindowFunction) :

        def process(
                self, 
                key: Types.STRING(), 
                context: ProcessWindowFunction.Context,
                counters: Iterable[tuple[str, float, int]]
            ) -> Iterable[tuple[int, str, float, tuple]] :
            
            counter = next(iter(counters))
            window : TimeWindow = context.window()
            
            yield (window.start, counter[0], counter[1], counter[2])




def getQuerySchema_JSON() -> JsonRowSerializationSchema:
    serialization_schema = JsonRowSerializationSchema \
        .builder() \
        .with_type_info(
            type_info=Types.ROW([Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.INT()])
        ) \
        .build()
    
    return serialization_schema
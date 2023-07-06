from pyflink.common.typeinfo import Types
from collections.abc import Iterable
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.window import TimeWindow
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction, ReduceFunction, KeyedProcessFunction, RuntimeContext
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.state import ValueStateDescriptor

from psquare.psquare import PSquare


class KeyedPercentileComputation(KeyedProcessFunction) :

    def __init__(self) -> None:
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(ValueStateDescriptor(
            "my_state", Types.PICKLED_BYTE_ARRAY()))
    
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):

        currState = self.state.value()
        if (currState == None) :
            #PSquare().p_estimate
            currState = [value[0], value[1], PSquare(0.25), PSquare(0.5), PSquare(0.75), 0]

        currState[2].update(value[2]) ## Update 25_per
        currState[3].update(value[2]) ## Update 50_per
        currState[4].update(value[2]) ## Update 75_per
        currState[5] += 1

        self.state.update(currState)
        
        ctx.timer_service().register_event_time_timer(value[0] + 30 * 60 * 1000)

    
    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):

        result = self.state.value()

        ## TODO Change to consider other windows size
        if (timestamp >= result[0] + 30 * 60 * 1000) :
            yield (result[0], result[1], result[2].p_estimate(), result[3].p_estimate(), result[4].p_estimate(), result[5])

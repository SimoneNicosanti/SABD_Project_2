from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from psquare.psquare import PSquare


class KeyedPercentileComputation(KeyedProcessFunction) :

    def __init__(self, millisecDuration : int) -> None:
        self.state = None
        self.milliseconds = millisecDuration


    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(ValueStateDescriptor(
            "my_state", Types.PICKLED_BYTE_ARRAY()))
    

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):

        currState = self.state.value()
        if (currState == None) : ## list of timestamp dict for values
            currState = [value[0], dict()]

        market = value[1]
        percDict : dict = currState[1]

        marketPerc = percDict.get(market)
        if (marketPerc == None) :
            marketPerc = [PSquare(25), PSquare(50), PSquare(75)]
        
        marketPerc[0].update(value[2])
        marketPerc[1].update(value[2])
        marketPerc[2].update(value[2])

        percDict[market] = marketPerc

        self.state.update(currState)
        
        ctx.timer_service().register_event_time_timer(currState[0] + self.milliseconds)

    
    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):

        result = self.state.value()
        
        if (timestamp == result[0] + self.milliseconds) :
            resultList = [result[0]]
            resultDict : dict = result[1]
            for market in sorted(resultDict.keys()) :
                marketPerc = resultDict[market]
                resultList.append(market)
                resultList.append(marketPerc[0].p_estimate())
                resultList.append(marketPerc[1].p_estimate())
                resultList.append(marketPerc[2].p_estimate())

            yield tuple(resultList)

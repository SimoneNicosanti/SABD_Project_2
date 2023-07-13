from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, AggregateFunction
from pyflink.datastream.state import ValueStateDescriptor

from psquare.psquare import PSquare
from tdigest import TDigest


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



class TDigestComputation(AggregateFunction) :

    def create_accumulator(self):
        return (0, dict()) ## (Timestamp, dictOfMarketDigest)
    

    def add(self, value, accumulator):
        ## Value is like (timestamp, ID, variation)

        market = value[1]
        digestDict : dict = accumulator[1]
        if (digestDict.get(market) == None) :
            digestDict[market] = TDigest()
        
        marketDigest : TDigest = digestDict[market]
        marketDigest.update(value[2])

        return (value[0], digestDict)


    def merge(self, acc_a, acc_b):

        digestDict_a : dict = acc_a[1]
        digestDict_b : dict = acc_b[1]

        resultDict = dict()
        for key_a in digestDict_a :
            if key_a in digestDict_b :
                resultDict[key_a] = digestDict_a[key_a] + digestDict_b[key_a]
            else :
                resultDict[key_a] = digestDict_a[key_a]
        
        for key_b in digestDict_b :
            if key_b not in digestDict_a :
                resultDict[key_b] = digestDict_b[key_b]


        return (acc_a[0], resultDict)
    

    def get_result(self, accumulator):

        resultList = [accumulator[0]]
        resultDict : dict = accumulator[1]
        for market in sorted(resultDict.keys()) :
            marketDigest : TDigest = resultDict[market]
            resultList.append(market)
            resultList.append(marketDigest.percentile(25))
            resultList.append(marketDigest.percentile(50))
            resultList.append(marketDigest.percentile(75))
        
        return tuple(resultList)
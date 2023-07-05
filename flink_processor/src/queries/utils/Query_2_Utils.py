from pyflink.common.typeinfo import Types
from collections.abc import Iterable
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.window import TimeWindow
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction, ReduceFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import AggregateFunction


class VariationReduceFunction(ReduceFunction) :

    def reduce(self, x, y):
        timestamp = x[0]
        minTime = min(x[1], y[1])
        maxTime = max(x[3], y[3])

        if x[1] == y[1] :
            minLast = min(x[2], y[2])
        else :
            minLast = x[2] if x[1] < y[1] else y[2]
        
        if (x[3] == y[3]) :
            maxLast = max(x[4], y[4])
        else :
            maxLast = x[4] if x[3] > y[3] else y[4]

        return (timestamp, minTime, minLast, maxTime, maxLast)


class MyProcessWindowFunction(ProcessWindowFunction) :

    def process(
            self, 
            key: Types.STRING(), 
            context: ProcessWindowFunction.Context,
            values: Iterable[tuple[str, float, float, float, float]]
        ) -> Iterable[tuple[int, str, float, tuple]] :
        
        value = next(iter(values))
        
        window : TimeWindow = context.window()
        yield (window.start, value[0], value[1], value[2], value[3], value[4])


class SecondTimestampAssigner(TimestampAssigner) :

## On Tuple like (ID, SecType, Last, timestamp)
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        valueTimeStamp = value[0]
        
        return valueTimeStamp
    

class FinalMapFunction(MapFunction) :
    ## On value like (windowStartTime, rankingOf(variation, ID))
    def map(self, value):

        finalList = [value[0]]
        for rankTuple in value[1] :
            finalList.append(rankTuple[1])
            finalList.append(rankTuple[0])

        return tuple(finalList)



class RankingFunction(AggregateFunction):

    def create_accumulator(self):
        return (0, []) ## (Timestamp, maxList, minList)
    

    def add(self, value, accumulator):
        ## Value is like (timestamp, ID, variation)
        timestamp = value[0]

        rankList : list = accumulator[1]
        rankList.append((value[2], value[1]))

        resultList = self.update_rank_list(rankList)

        return (timestamp, resultList)


    def merge(self, acc_a, acc_b):

        mergedList = acc_a[1] + acc_b[1]
        
        resultList = self.update_rank_list(mergedList)

        return (acc_a[0], resultList)
    

    def get_result(self, accumulator):
        return accumulator


    def update_rank_list(self, rankList : list) -> list :
        sortedRankList = sorted(
            rankList,
            reverse = True
        )

        if (len(sortedRankList) > 10) :
            resultList = sortedRankList[0 : 5] + sortedRankList[-5 : ]
        else :
            resultList = sortedRankList

        return resultList
    


def getQuerySchema_JSON() -> JsonRowSerializationSchema:
    serialization_schema = JsonRowSerializationSchema \
        .builder() \
        .with_type_info(
            type_info=Types.ROW(
                [Types.FLOAT()] + [Types.STRING(), Types.FLOAT()] * 10
            )
        ) \
        .build()
    
    return serialization_schema
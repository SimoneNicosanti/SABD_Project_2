from engineering import DataStreamFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Time, WatermarkStrategy

from pyflink.common import Row
from pyflink.datastream.window import TumblingEventTimeWindows

from engineering import SinkFactory

from queries.utils.MyTimestampAssigner import MyTimestampAssigner

from pyflink.common.typeinfo import Types

from queries.utils.Query_2_Utils import VariationReduceFunction, MyProcessWindowFunction, SecondTimestampAssigner
from queries.utils.Query_3_Utils import KeyedPercentileComputation

import json


def query() :

    (dataStream, env) = DataStreamFactory.getDataStream() ## (ID, SecType, Last, Timestamp)

    partialStream = dataStream.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps(
        ).with_timestamp_assigner(
            MyTimestampAssigner()
        )
    ).map( ## (ID, Time_1, Last_1, Time_2, Last_2)
        lambda x : (x[0], x[3], x[2], x[3], x[2])
    ).key_by(
        lambda x : x[0]
    )

    windowList = {
        "Query_3_Min" : Time.minutes(30),
        "Query_3_Hour" : Time.hours(1),
        "Query_3_Day" : Time.days(1)
    }
    
    for key, timeDuration in windowList.items() :
        tumblingWindow = TumblingEventTimeWindows.of(timeDuration)
        millisecDuration = timeDuration.to_milliseconds()

        partialStream.window(
                tumblingWindow
        ).reduce( 
            VariationReduceFunction(),
            MyProcessWindowFunction() ## (windowStart, ID, minTime, minLast, maxTime, maxLast)
        ).filter(
            lambda x : (x[2] != x[4]) or (x[2] == x[4] and x[3] != x[5])
        ).map( ## (windowStart, ID, variation)
            lambda x : (x[0], x[1], x[5] - x[3])
        ).map( ## (windowStart, market, variation)
            lambda x: (x[0], str(x[1])[str(x[1]).index(".") + 1 : ], x[2])
        ).assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps(
            ).with_timestamp_assigner(
                SecondTimestampAssigner()
            )
        ).key_by( ## Keyed by windowStart
            lambda x : x[0]
        ).process( ## (timestamp, market, 25_perc, 50_perc, 75_perc)
            KeyedPercentileComputation(millisecDuration)
        ).map( ## Convertion for kafka save
            lambda x : json.dumps(x) ,
            output_type = Types.STRING()
        ).sink_to(
            SinkFactory.getKafkaSink(key)
        )
    
    env.execute("Query_3")

    return 
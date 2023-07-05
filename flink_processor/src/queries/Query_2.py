from engineering import DataStreamFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Time, WatermarkStrategy

from pyflink.common import Row
from pyflink.datastream.window import TumblingEventTimeWindows

from engineering import SinkFactory

from queries.utils.MyTimestampAssigner import MyTimestampAssigner

from pyflink.common.typeinfo import Types

from queries.utils.Query_2_Utils import MyProcessWindowFunction, SecondTimestampAssigner, FinalMapFunction, getQuerySchema_JSON, RankingFunction

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
        "Query_2_Min" : Time.minutes(30),
        "Query_2_Hour" : Time.hours(1),
        "Query_2_Day" : Time.days(1)
    }
    
    for key, timeDuration in windowList.items() :
        tumblingWindow = TumblingEventTimeWindows.of(timeDuration)

        partialStream.window(
                tumblingWindow
        ).reduce( 
            lambda x, y : ( ## (ID, minTime, minLast, maxTime, maxLast)
                    x[0], 
                    min(x[1], y[1]),
                    x[2] if x[1] < y[1] else y[2],
                    max(x[3], y[3]),
                    x[4] if x[3] > y[3] else y[4]
                    ),
                MyProcessWindowFunction() ## (windowStart, ID, minTime, minLast, maxTime, maxLast)
        ).filter(
            lambda x : x[1] != x[3]
        ).map( ## (windowStart, ID, variation)
            lambda x : (x[0], x[1], x[5] - x[3])
        ).assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps(
            ).with_timestamp_assigner(
                SecondTimestampAssigner()
            )
        ).key_by(
            lambda x : x[0]
        ).window(
            tumblingWindow
        ).aggregate( ## (startWindow, sortedListOf((variation, ID)))
            RankingFunction()
        ).map( ## (startWindowTimestamp, ID_i, Variation_i)
            FinalMapFunction()
        ).map( ## Row(startWindowTimestamp, ID_i, Variation_i)
            lambda x : Row(x[0], 
                            x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10],
                            x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], x[20]
                            ),
            output_type = Types.ROW([Types.FLOAT()] + [Types.STRING(), Types.FLOAT()] * 10)
        ).sink_to(
            SinkFactory.getKafkaSink(key, getQuerySchema_JSON())
        )
    
    env.execute("Query_2")

    return


def query_variant() :
    ## With global sorting and not incremental ranking
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
        "Query_2_Min" : TumblingEventTimeWindows.of(Time.minutes(30)),
        "Query_2_Hour" : TumblingEventTimeWindows.of(Time.hours(1)),
        "Query_2_Day" : TumblingEventTimeWindows.of(Time.days(1))}
    
    for key, tumblingWindow in windowList.items() :
        partialStream.window(
                tumblingWindow
        ).reduce( 
            lambda x, y : ( ## (ID, minTime, minLast, maxTime, maxLast)
                    x[0], 
                    min(x[1], y[1]),
                    x[2] if x[1] < y[1] else y[2],
                    max(x[3], y[3]),
                    x[4] if x[3] > y[3] else y[4]
                    ),
                MyProcessWindowFunction() ## (windowStart, ID, minTime, minLast, maxTime, maxLast)
        ).filter(
            lambda x : x[1] != x[3]
        ).map( ## (windowStart, ID, variation)
            lambda x : (x[0], x[1], x[5] - x[3])
        ).map( ## (windowStart, [variation, ID])
            lambda x : (x[0], [(x[2], x[1])])
        ).assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps(
            ).with_timestamp_assigner(
                SecondTimestampAssigner()
            )
        ).window_all( ## Rewindow using startWindowTimestamp as timestamp
            tumblingWindow
        ).reduce( ## (startWindowTimestamp, listOf(variation, ID))
            lambda x, y : (x[0], x[1] + y[1])
        ).map( ## (startWindowTimestamp, sortedListOf(variation, ID))
            lambda x : (x[0], sorted(x[1], reverse = True))
        ).map( ## (startWindowTimestamp, rankingOf(variation, ID))
            lambda x : (
                x[0], 
                x[1][0 : 5] + x[1][-5 : ] if len(x[1]) >= 10 else x[1]
                )
        ).map( ## (startWindowTimestamp, ID_i, Variation_i)
            FinalMapFunction()
        ).map( ## Row(startWindowTimestamp, ID_i, Variation_i)
            lambda x : Row(x[0], 
                            x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10],
                            x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], x[20]
                            ),
            output_type = Types.ROW([Types.FLOAT()] + [Types.STRING(), Types.FLOAT()] * 10)
        ).sink_to(
            SinkFactory.getKafkaSink(key, getQuerySchema_JSON())
        )
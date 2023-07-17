from engineering import DataStreamFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Time, WatermarkStrategy

from pyflink.common import Row
from pyflink.datastream.window import TumblingEventTimeWindows

from engineering import SinkFactory

import json

from queries.utils.MyTimestampAssigner import MyTimestampAssigner

from pyflink.common.typeinfo import Types

from queries.utils.Query_1_Utils import MyProcessWindowFunction
from queries.utils.MetricsTaker import MetricsTaker

import datetime


def query(evaluate = False) :
    
    (dataStream, env) = DataStreamFactory.getDataStream() ## (ID, SecType, Last, Timestamp)

    if (evaluate) :
        env.get_config().set_latency_tracking_interval(10)
    
    partialStream = dataStream.filter(
            lambda x : str(x[0]).startswith("G") and str(x[0]).endswith(".FR") and str(x[1]) == "E"
        ).assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps(
            ).with_timestamp_assigner(
                MyTimestampAssigner()
            )
        ).map( ## (ID, Last, 1)
            lambda x : (x[0], x[2], 1),
            output_type = Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.INT()])
        ).key_by( ## (ID, Last, 1) keyd by ID
            key_selector = lambda x : x[0]
        )
    
    windowedStreams = {
        "Query_1_Hour" : TumblingEventTimeWindows.of(Time.hours(1)), 
        "Query_1_Day" : TumblingEventTimeWindows.of(Time.days(1)), 
        "Query_1_Glb" : TumblingEventTimeWindows.of(Time.days(7), Time.days(4)) ## The delay is to start from 8-11-2021
        }
    

    for key, window in windowedStreams.items() :
        partialStream.window(
            window
        ).reduce( 
            lambda x, y : (x[0], x[1] + y[1], x[2] + y[2]), ## (ID, total, count)
            MyProcessWindowFunction() ## (windowStart, ID, total, count)
        ).map( ## (windowStart, ID, avgLast, count)
            lambda x : (x[0], x[1], x[2] / x[3], x[3])
        ).map( ## Convertion for kafka save
            lambda x : json.dumps(x) ,
            output_type = Types.STRING()
        ).map(
            func = MetricsTaker(),
            output_type = Types.STRING()
        ).name(
            key + "_Metrics"
        ).sink_to(
            SinkFactory.getKafkaSink(key)
        )

        if (evaluate) :
            env.execute_async(key)
    
    if (not evaluate) :
        env.execute("Query_1")

    return

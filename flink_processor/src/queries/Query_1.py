from engineering import DataStreamFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Time, WatermarkStrategy

from pyflink.common import Row
from pyflink.datastream.window import TumblingEventTimeWindows

from engineering import SinkFactory

from queries.utils.MyTimestampAssigner import MyTimestampAssigner

from pyflink.common.typeinfo import Types

from queries.utils.Query_1_Utils import MyProcessWindowFunction, getQuerySchema_JSON


def query() :
    
    (dataStream, env) = DataStreamFactory.getDataStream() ## (ID, SecType, Last, Timestamp)
    
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
        "Query_1_Glb" : TumblingEventTimeWindows.of(Time.days(7))
        }
    

    for key, tumblingWindow in windowedStreams.items() :
        partialStream.window(
            tumblingWindow
        ).reduce( 
            lambda x, y : (x[0], x[1] + y[1], x[2] + y[2]), ## (ID, total, count)
            MyProcessWindowFunction() ## (windowStart, ID, total, count)
        ).map( ## (windowStart, ID, avgLast, count)
            lambda x : (x[0], x[1], x[2] / x[3], x[3])
        ).map( ## Convertion for kafka save
            lambda x : Row(x[0], x[1], x[2], x[3]) ,
            output_type = Types.ROW([Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.INT()])
        ).sink_to(
            SinkFactory.getKafkaSink(key, getQuerySchema_JSON())
        )
    

    env.execute("Query_1")

    return
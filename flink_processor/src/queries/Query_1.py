from engineering import DataStreamFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Time, WatermarkStrategy, Duration

from pyflink.common import Row
from pyflink.datastream.window import TumblingEventTimeWindows

from engineering import SinkFactory

from queries.utils.GlobalTrigger import GlobalTrigger
#from queries.utils.MyTimestampAssigner import MyTimestampAssigner
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import GlobalWindows

from pyflink.common.typeinfo import Types

from engineering import SerializationSchemaFactory


def query() :

    class MyTimestampAssigner(TimestampAssigner) :
        def extract_timestamp(self, value, record_timestamp: int) -> int:
            valueTimeStamp = value[1][2]
            
            return valueTimeStamp


    (dataStream, env) = DataStreamFactory.getDataStream()
    
    partialStream = dataStream.filter(
            lambda x : str(x[0]).startswith("G") and str(x[0]).endswith(".FR") and str(x[1]) == "E"
        ).map( ## (ID, (Last, 1, Timestamp))
            func = lambda x : (x[0] , (x[2], 1, x[3])),
            output_type = Types.TUPLE([
                Types.STRING() , 
                Types.TUPLE([Types.FLOAT(), Types.INT(), Types.FLOAT()])
                ])
        ).assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps(
            ).with_timestamp_assigner(
                MyTimestampAssigner()
            )
        ).key_by( ## (ID, (Last, 1, Timestamp))
            key_selector = lambda x : x[0],
            key_type = Types.STRING()
        )
    

    serializationSchema = SerializationSchemaFactory.getQueryOneSchema()


    firstResultStream = partialStream.window(
            TumblingEventTimeWindows.of(Time.hours(1))
        ).reduce( ## (ID, (sumLast, count, minTimestamp))
            lambda x, y : (x[0], (x[1][0] + y[1][0], x[1][1] + y[1][1], min(x[1][2], y[1][2])))
        ).map( ## (timestamp, ID, avgLast, count)
            lambda x : (x[1][2], x[0], x[1][0] / x[1][1], x[1][1])
        ).map(
            lambda x : Row(x[0], x[1], x[2], x[3]) ,
            output_type = Types.ROW([Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.INT()])
        )
    
    firstResultStream.sink_to(SinkFactory.getKafkaSink("Query_1_Hour", serializationSchema))
    firstResultStream.print()
    

    secondResultStream = partialStream.window(
            TumblingEventTimeWindows.of(Time.days(1))
        ).reduce(
            lambda x, y : (x[0], (x[1][0] + y[1][0], x[1][1] + y[1][1], min(x[1][2], y[1][2])))
        ).map( ## (timestamp, ID, avgLast, count)
            lambda x : (x[1][2], x[0], x[1][0] / x[1][1], x[1][1])
        ).map( ## Prapared for save
            lambda x : Row(x[0], x[1], x[2], x[3]) ,
            output_type = Types.ROW([Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.INT()])
        )
    
    secondResultStream.sink_to(SinkFactory.getKafkaSink("Query_1_Day", serializationSchema))
    
    
    thirdResultStream = partialStream.window(
            GlobalWindows.create()
        ).trigger(
            GlobalTrigger()
        ).reduce(
            lambda x, y : (x[0], (x[1][0] + y[1][0], x[1][1] + y[1][1], min(x[1][2], y[1][2])))
        ).map(
            lambda x : (x[1][2], x[0], x[1][0] / x[1][1], x[1][1])
        ).map( ## Prapared for save
            lambda x : Row(x[0], x[1], x[2], x[3]) ,
            output_type = Types.ROW([Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.INT()])
        )
    
    thirdResultStream.sink_to(SinkFactory.getKafkaSink("Query_1_Day", serializationSchema))
    thirdResultStream.print()

    env.execute("Query_1")
    env.close()

    return



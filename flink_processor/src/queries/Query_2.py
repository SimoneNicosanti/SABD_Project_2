from engineering import DataStreamFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Time, WatermarkStrategy, Duration

from pyflink.common import Row
from pyflink.datastream.window import TumblingEventTimeWindows

from engineering import SinkFactory

from queries.utils.GlobalTrigger import GlobalTrigger
from queries.utils.MyTimestampAssigner import MyTimestampAssigner
from pyflink.datastream.window import GlobalWindows

from pyflink.common.typeinfo import Types

from engineering import SerializationSchemaFactory

def query() :

    (dataStream, env) = DataStreamFactory.getDataStream() ## (ID, SecType, Last, Timestamp)

    partialStream = dataStream.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps(
        ).with_timestamp_assigner(
            MyTimestampAssigner()
        )
    ).window_all(
        TumblingEventTimeWindows.of(Time.minutes(30))
    ).process()


    env.execute("Query_2")

    return
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, DataStream, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.formats.json import JsonRowDeserializationSchema


def getEnv() :
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    env.add_jars("file:///KafkaConnectorDependencies.jar")
    # env.add_jars("file:///opt/flink/lib/flink-metrics-prometheus-1.17.1.jar")
    
    env.add_python_file("file:///src/queries/utils/MyTimestampAssigner.py")
    env.add_python_file("file:///src/queries/utils/Query_1_Utils.py")
    env.add_python_file("file:///src/queries/utils/Query_2_Utils.py")
    env.add_python_file("file:///src/queries/utils/Query_3_Utils.py")
    env.add_python_file("file:///src/queries/utils/MetricsTaker.py")


    return env

from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.common.configuration import Configuration

class MetricsTaker(MapFunction):
    def __init__(self):
        self.latency = None
        self.throughput = None

    def open(self, runtime_context: RuntimeContext):
        # an average rate of events per second over 120s, default is 60s.
        # self.latency = runtime_context \
        #     .get_metrics_group() \
        #     .gauge(
        #         "latency", 
        #         lambda: self.latency 
        #     )
        
        self.throughput = runtime_context \
            .get_metrics_group() \
            .meter(
                "my_meter", 
                time_span_in_seconds = 1
            )

    def map(self, value):

        ## Update for throughput
        self.throughput.mark_event()
        return value

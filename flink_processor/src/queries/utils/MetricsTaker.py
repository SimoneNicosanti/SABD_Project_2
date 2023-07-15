from typing import Any
from pyflink.datastream.functions import MapFunction, RuntimeContext
from datetime import datetime
from typing import Callable

class MetricsTaker(MapFunction):
    def __init__(self):
        self.latency = 0.0
        self.throughput = 0.0
        self.counter = 0
        self.startTime = 0


    def open(self, runtime_context: RuntimeContext):
        ## Time of creation for this operator
        self.startTime = datetime.now().timestamp() ## Timestamp di creazione in secondi
        self.counter = 0

        runtime_context \
            .get_metrics_group() \
            .gauge(
                "latency", 
                lambda: self.latency * 100000
            )
        
        runtime_context \
            .get_metrics_group() \
            .gauge(
                "throughput", 
                lambda: self.throughput * 100000
            )
        

    def map(self, value):

        self.counter += 1
        nowTimestamp = datetime.now().timestamp()
        diffTime = nowTimestamp - self.startTime

        ## Average latency for tuple [s / tuple]
        self.latency = diffTime / self.counter 

        ## Average throughput [tuple / s]
        self.throughput = self.counter / diffTime

        return value

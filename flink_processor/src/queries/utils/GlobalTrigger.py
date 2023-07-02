from pyflink.datastream.window import Trigger, TriggerResult
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

import datetime

class GlobalTrigger(Trigger) :

        def on_element(self, element: tuple, timestamp: int, window, ctx: 'Trigger.TriggerContext') -> TriggerResult:
            return TriggerResult.CONTINUE
            
        def on_processing_time(self, time: int, window, ctx: 'Trigger.TriggerContext') -> TriggerResult:
            return TriggerResult.CONTINUE
        
        def on_event_time(self, time: int, window, ctx: 'Trigger.TriggerContext') -> TriggerResult:
            limitTime = datetime.datetime(2021, 11, 19, 12, 0, 0, 0)
            limitTimeTimestamp = datetime.datetime.timestamp(limitTime) * 1000
            if time > limitTimeTimestamp :
                return TriggerResult.FIRE
            else :
                return TriggerResult.CONTINUE
        
        def on_merge(self, window, ctx: 'Trigger.OnMergeContext') -> None:
            return TriggerResult.CONTINUE
        
        def clear(self, window, ctx) -> None:
            return super().clear(window, ctx)
        
from pyflink.datastream import DataStream
from engineering import DataStreamFactory
from engineering import FlinkEnvFactory
from pyflink.common.typeinfo import Types
import json
import datetime

import enum

class Duration(enum.Enum) :
    HOUR = 1
    DAY = 2
    GLOBAL = 3


def query(duration : Duration = Duration.HOUR) :

    def jsonToTuple(mess : str) :
        jsonObject = json.loads(mess)

        return (datetime.datetime.strptime(jsonObject["Date"], "%d-%m-%Y"), 
                jsonObject["Time"], 
                jsonObject["ID"], 
                jsonObject["SecType"], 
                float(jsonObject["Last"]),
                datetime.datetime.strptime(jsonObject["TradingDate"], "%d-%m-%Y"),
                jsonObject["TradingTime"])

    (dataStream, env) = DataStreamFactory.getDataStream()
    
    dataStream.map(
        jsonToTuple,
        output_type = Types.TUPLE([Types.SQL_DATE(), Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.SQL_DATE(), Types.STRING()])
        ).filter(
            lambda x : str(x[2]).startswith("G") and str(x[2]).endswith(".FR") and str(x[3]) == "E"
        )
    
    # .map(
    #     lambda mess : (mess[0], mess[1], mess[2], mess[3], mess[4], mess[5], mess[6])
    # )

    env.execute("Kafka Attempt")

    env.close()

    return


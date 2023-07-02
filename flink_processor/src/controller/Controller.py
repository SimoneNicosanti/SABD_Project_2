from engineering import FlinkEnvFactory
from pyflink.datastream import StreamExecutionEnvironment
from queries import Query_1

def controller(queryNum : int) :

    ## TODO Capire come fare la preelaborazione

    if (queryNum == 1) :
        Query_1.query()
    # elif (queryNum == 2) :
    #     Query_2.query()
    # elif (queryNum == 3) :
    #     Query_3.query()
    # else :
    #     Query_1.query()
    #     Query_2.query()
    #     Query_3.query()

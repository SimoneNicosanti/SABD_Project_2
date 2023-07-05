
from queries import Query_1, Query_2, Query_3

def controller(queryNum : int) :

    ## TODO Capire come fare la preelaborazione. Spostare filtarggio delle tuple in flink alla lettura dello stream

    if (queryNum == 1) :
        Query_1.query()
    elif (queryNum == 2) :
        Query_2.query()
    elif (queryNum == 3) :
        Query_3.query()
    # else :
    #     Query_1.query()
    #     Query_2.query()
    #     Query_3.query()

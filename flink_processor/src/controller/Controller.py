
from queries import Query_1, Query_2, Query_3

def controller(queryNum : int, evaluate = False) :

    if (queryNum == 1) :
        Query_1.query(evaluate)
    elif (queryNum == 2) :
        Query_2.query(evaluate)
    elif (queryNum == 3) :
        Query_3.query(evaluate)
    # else :
    #     Query_1.query()
    #     Query_2.query()
    #     Query_3.query()

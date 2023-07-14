from controller import Consumer
import os
import sys


def main() :

    if (len(sys.argv)) == 1 :
        print("Errore parametri: Inserire il numero della query")
        return
    
    queryNum = sys.argv[1]
    initEnv(queryNum)
    
    Consumer.consume()
    return

def initEnv(queryNum : int) :
    for fileName in os.listdir("/Results"):
        if fileName.startswith("Query_" + str(queryNum)):
            os.remove(os.path.join("/Results", fileName))


if __name__ == "__main__" :
    main()
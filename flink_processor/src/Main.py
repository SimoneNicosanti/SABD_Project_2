import sys
from controller import Controller

def main() :

    queryNum = int(sys.argv[1])
    evaluate = (sys.argv[2] == "y")
    Controller.controller(queryNum, evaluate)
    
    return


if __name__ == "__main__" :
    main()
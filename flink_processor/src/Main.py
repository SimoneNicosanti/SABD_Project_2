import sys
from controller import Controller

def main() :

    queryNum = int(sys.argv[1])
    Controller.controller(queryNum)
    
    
    return


if __name__ == "__main__" :
    main()
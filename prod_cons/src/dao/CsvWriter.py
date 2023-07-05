import json
import os
import shutil
import csv

def writeCsv(topic : str, value : str) :
    dictValue = json.loads(value)

    if (topic.startswith("Query_1")) :
        header = ["Timestamp", "ID", "Avg", "Count"]
    elif (topic.startswith("Query_2")) :
        header = ["Timestamp"]
        for i in range(1, 11) :
            header.append("ID_" + str(i))
            header.append("Var_" + str(i))
    else :
        pass

    writeQueryResult(topic, dictValue, header)

    return


def writeQueryResult(topic : str, value : dict, header : list) :
    filePath = os.path.join("/Results", topic + ".csv")
    if (not os.path.exists(filePath)) :
        with open(filePath, "+x") as file :
            csvWriter = csv.writer(file)
            csvWriter.writerow(header)
    
    with open(filePath, "+a") as file :
        csvWriter = csv.writer(file)
        csvWriter.writerow(value.values())
    
    return

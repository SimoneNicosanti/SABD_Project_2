import json
import os
import csv

def writeCsv(topic : str, value : str) :
    valueList = json.loads(value)

    if (topic.startswith("Query_1")) :
        header = ["Timestamp", "ID", "Avg", "Count"]
    elif (topic.startswith("Query_2")) :
        header = ["Timestamp"]
        for i in range(1, 11) :
            header.append("ID_" + str(i))
            header.append("Var_" + str(i))
    elif (topic.startswith("Query_3")) :
        header = ["Timestamp", "Market", "25_Perc", "50_Perc", "75_Perc"]

    writeQueryResult(topic, valueList, header)

    return


def writeQueryResult(topic : str, values : list, header : list) :
    filePath = os.path.join("/Results", topic + ".csv")
    if (not os.path.exists(filePath)) :
        with open(filePath, "+x") as file :
            csvWriter = csv.writer(file)
            csvWriter.writerow(header)
        os.chmod(filePath, 0o777)
    
    with open(filePath, "+a") as file :
        csvWriter = csv.writer(file)
        csvWriter.writerow(values)
    
    return

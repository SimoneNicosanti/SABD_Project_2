from engineering import KafkaSingleton
import time
import json
import datetime
from dao import CsvReader

SCALE_FACTOR = 3600

def produce() :

    kafkaProducer = KafkaSingleton.getKafkaProducer()
    kafkaTopic = KafkaSingleton.getKafkaTopic()

    header = ["Date", "Time", "ID", "SecType", "Last", "TradingTime", "TradingDate"]

    eventList = CsvReader.readDatasetFromCSV("../dataset/Dataset.csv")

    prevTime : datetime.datetime = None
    print("Sending Data")

    i = 0
    idsSet = set()
    for event in eventList :

        eventInfo = [event[2], event[3], event[0], event[1], event[21], event[23], event[26]]
        
        dictData = {header[i] : eventInfo[i] for i in range(0, len(header))}

        idsSet.add((dictData["ID"], dictData["SecType"]))

        rowTimeString = str(eventInfo[0]) + " " + eventInfo[1]
        rowTime = datetime.datetime.strptime(rowTimeString, '%d-%m-%Y %H:%M:%S.%f')

        if (prevTime != None) :
            timeDiff = rowTime - prevTime
            totSec = timeDiff.total_seconds()

            sleepPeriod = totSec / SCALE_FACTOR
            time.sleep(sleepPeriod)

        prevTime = rowTime
             
        kafkaProducer.send(
            topic = kafkaTopic,
            value = json.dumps(dictData).encode()
        )

        i += 1
        if (i == 1000000) :
            break

        print(json.dumps(dictData).encode())


    for couple in idsSet :
        endTuple = {"Date" : "", "Time" : "", "ID" : couple[0] , "SecType" : couple[1], "Last" : 0, "TradingTime" : "12:00:00.000", "TradingDate" : "20-11-2021"}
        kafkaProducer.send(
            topic = kafkaTopic,
            value = json.dumps(endTuple).encode()
        )

        # print(json.dumps(endTuple).encode())

    ## TODO Find a way to trigger last queries based on variation
    ## To trigger last part for queries based on variation
    for couple in idsSet :
        endTuple = {"Date" : "", "Time" : "", "ID" : couple[0] , "SecType" : couple[1], "Last" : 0, "TradingTime" : "12:25:00.000", "TradingDate" : "20-11-2021"}
        kafkaProducer.send(
            topic = kafkaTopic,
            value = json.dumps(endTuple).encode()
        )

        # print(json.dumps(endTuple).encode())

    kafkaProducer.close()
    
    return
import csv
import datetime

# def readDatasetFromCSV(fileName : str) -> list :
#     eventList = []

#     with open('../dataset/Dataset.csv') as datasetFile:
#         reader = csv.reader(datasetFile)
                
#         # Iterate over each row in the csv 
#         # file using reader object
#         for row in reader :
#             ## Skip raw text header
#             if (row[0].startswith("#")) :
#                 continue
            
#             ## Skip header
#             if (row[1] == "SecType") :
#                 continue

#             currDate = datetime.datetime.strptime(row[2], '%d-%m-%Y').date() #.strptime(rowTimeString, '%d-%m-%Y %H:%M:%S.%f')
#             print(currDate)
#             ## Date, Time, ID, SecType, Last, TradingTime, TradingDate
#             rowData = [currDate, row[3], row[0], row[1], row[21], row[23], row[26]]

#             eventList.append(rowData)
    
#     return eventList

def readDatasetFromCSV(fileName : str) -> list :
    with open('../dataset/Dataset.csv') as datasetFile:
        reader = csv.reader(datasetFile)

        filteredList = filter(lambda row : not row[0].startswith("#"), reader)
        filteredList = filter(lambda row : row[1] != "SecType", filteredList)
        filteredList = filter(lambda row : row[26] != "", filteredList)
        filteredList = filter(lambda row : row[23] != "00:00:00.000", filteredList)

        sortedList = sorted(
            filteredList,
            key = lambda row : row[2] + row[3]
        )
    
    return sortedList
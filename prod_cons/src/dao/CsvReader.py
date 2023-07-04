import csv
import datetime

def readDatasetFromCSV(fileName : str) -> list :
    with open('../dataset/Dataset.csv') as datasetFile:
        reader = csv.reader(datasetFile)

        filteredList = filter(lambda row : not row[0].startswith("#"), reader) ## Header Removal
        filteredList = filter(lambda row : row[1] != "SecType", filteredList) ## Header Removal
        filteredList = filter(lambda row : row[26] != "", filteredList)
        filteredList = filter(lambda row : row[23] != "00:00:00.000", filteredList)

        sortedList = sorted(
            filteredList,
            key = lambda row : row[2] + row[3]
        )
    
    return sortedList
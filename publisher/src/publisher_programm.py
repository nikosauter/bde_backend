import csv
import time
import json

#read csv-file
data = open('/opt/venv/src/twitter_sample_data_small.csv', 'r')
data = csv.reader(data)

#create list with keys for json generation
keys = ["ID", "Timestamp", "Username", "Text"]

#go through csv-file
for row in data:
    #remove first and third column
    row.pop(0)
    row.pop(2)
    #make dicionary with keys and row for json generation
    dictionary = dict(zip(keys, row))

    #generate json
    json_object = json.dumps(dictionary, indent = 4)
    print(json_object)
    time.sleep(.5)
    break
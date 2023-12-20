import csv
import time
import json
from confluent_kafka import Producer

#set up producer with batch.size = 1 so that every message will be directly send to kafka
producer = Producer({'bootstrap.servers': 'kafka:9092', "client.id" :'publisher', "batch.size": 1})

#read csv-file
data = open('/opt/venv/src/twitter_sample_data_small.csv', 'r')
data = csv.reader(data)

#create list with keys for json generation
keys = ["id", "timestamp", "username", "text"]

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

    #send json to kafka (no key specified -> use round robin for paritions which is fine as each post can stand for itself and the order is not really relevant)
    #producer.produce(topic="posts", value=json_object.encode('utf-8'))

    time.sleep(.5)
import csv
import pandas as pd
import math
import datetime
import os
import mmap
from pymongo import MongoClient
import urllib.request


client = MongoClient()
db = client['faster']
collection = db['small']




file_name = "web_csv_files/big.csv"
file_url = "http://35.241.167.153/small.csv"

urllib.request.urlretrieve(file_url, file_name)
rows = []
with open(file_name, 'r', encoding="utf8") as file:
    csvreader = csv.reader(file)
    found_row  = []
    time_before_running = datetime.datetime.now()
    for row in csvreader:
        address = { 
            'index' : row[0],
            'localid' : row[1],
            'table_name' : row[2],
            'fulladdress' : row[3],
            'city_id' : row[4],
        }
        collection.insert_one(address).inserted_id
        if '000205600VK56E' in row:
            print(row)
            found_row = row
        print(row, '\n--------------\n')
        print(found_row)
        os.system('cls')

time_before_running = datetime.datetime.now()
print(collection.find_one({"localid": "3281901VK4738A"}))
print('mongo: ', datetime.datetime.now() - time_before_running)

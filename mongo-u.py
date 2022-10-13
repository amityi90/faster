import csv
import math
import datetime
import os
from pymongo import MongoClient
import urllib.request
import re


client = MongoClient()
db = client['faster']
collection = db['big']




file_name = "web_csv_files/big.csv"
file_url = "http://35.241.167.153/big.csv"

# urllib.request.urlretrieve(file_url, file_name)
# rows = []
# with open(file_name, 'r', encoding="utf8") as file:
#     csvreader = csv.reader(file)
#     for row in csvreader:
#         address = { 
#             'index' : row[0],
#             'localid' : row[1],
#             'table_name' : row[2],
#             'fulladdress' : row[3],
#             'city_id' : row[4],
#         }
#         collection.insert_one(address).inserted_id
#         print(row, '\n--------------\n')
#         os.system('cls')

time_before_running = datetime.datetime.now()
print(collection.find_one({"localid": "1397003TK7519N"}))
print('mongo: ', datetime.datetime.now() - time_before_running)
s = collection.find({})
print(len(s))

# urllib.request.urlretrieve(file_url, file_name)
# rows = []
# with open(file_name, 'r', encoding="utf8") as file:
#     csvreader = csv.reader(file)
#     for row in csvreader:
#         address = { 
#             'index' : row[0],
#             'localid' : row[1],
#             'table_name' : row[2],
#             'fulladdress' : row[3],
#             'city_id' : row[4],
#         }
#         if re.match(r"[0-9]+[A-Z][A-Z]?[0-9]+[A-Z][A-Z]?", address['localid']):
#             letters_index = re.search(r"[A-Z][A-Z]?", address['localid'])
#             cluster_name = address['localid'][letters_index:]
#         elif re.match(r"[0-9]+[A-Z][A-Z]?[0-9]+", address['localid']):
#             letters_index = re.search(r"[A-Z][A-Z]?", address['localid'])
#             cluster_name = address['localid'][:letters_index + 1]
#         else:
#             cluster_name = address['localid'][:3]
#         print(cluster_name)
#         collection.insert_one(address).inserted_id
#         print(row, '\n--------------\n')
#         os.system('cls')

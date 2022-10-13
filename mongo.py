import collections
import csv
from venv import create
import pandas as pd
import math
import datetime
import os
import mmap
from pymongo import MongoClient
import urllib.request
import re


client = MongoClient()
db = client['faster']
# collection = db['small']




file_name = "web_csv_files\\small.csv"

# rows = []
# with open(file_name, 'r', encoding="utf8") as file:
#     csvreader = csv.reader(file)
#     found_row  = []
#     time_before_running = datetime.datetime.now()
#     for row in csvreader:
#         address = { 
#             'index' : row[0],
#             'localid' : row[1],
#             'table_name' : row[2],
#             'fulladdress' : row[3],
#             'city_id' : row[4],
#         }
#         collection.insert_one(address).inserted_id
#         if '000205600VK56E' in row:
#             found_row = row
#         print(row)
#         print(row, '\n--------------\n')
#         os.system('cls')

# time_before_running = datetime.datetime.now()
# print(collection.find_one({"localid": "3281901VK4738A"}))
# print('mongo: ', datetime.datetime.now() - time_before_running)

# rows = []
# with open(file_name, 'r', encoding="utf8") as file:
#     csvreader = csv.reader(file)
#     for i, row in enumerate(csvreader):
#         collection_name = ''
#         if i == 0:
#             continue
#         address = { 
#             'index' : row[0],
#             'localid' : row[1],
#             'table_name' : row[2],
#             'fulladdress' : row[3],
#             'city_id' : row[4],
#         }
#         if re.match(r"[0-9]+[A-Z][A-Z]?[0-9]+[A-Z][A-Z]?", address['localid']):
#             letters_index = re.search(r"[A-Z][A-Z]?", address['localid'])
#             letters_index = letters_index.span()[0]
#             collection_name = address['localid'][letters_index:]
#         elif re.match(r"[0-9]+[A-Z][A-Z]?[0-9]+", address['localid']):
#             letters_index = re.search(r"[A-Z][A-Z]?", address['localid'])
#             letters_index = letters_index.span()[1]
#             collection_name = address['localid'][:letters_index]
#         else:
#             collection_name = address['localid'][:3]
#         print(row, '\n', collection_name)

#         collection = db[collection_name]

#         collection.insert_one(address).inserted_id
#         print('\n--------------\n')


def find_address(address):
    collection_name = ''
    if re.match(r"[0-9]+[A-Z][A-Z]?[0-9]+[A-Z][A-Z]?", address):
        letters_index = re.search(r"[A-Z][A-Z]?", address)
        letters_index = letters_index.span()[0]
        collection_name = address[letters_index:]
    elif re.match(r"[0-9]+[A-Z][A-Z]?[0-9]+", address):
        letters_index = re.search(r"[A-Z][A-Z]?", address)
        letters_index = letters_index.span()[1]
        collection_name = address[:letters_index]
    else:
        collection_name = address[:3]
    collection = db[collection_name]
    return collection.find_one({"localid": address})

time_before_running = datetime.datetime.now()
print(find_address("0565106VK4706F"))
print('mongo: ', datetime.datetime.now() - time_before_running)


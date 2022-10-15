import csv
import math
import datetime
import os
from pymongo import MongoClient
import urllib.request
import re


client = MongoClient()
db = client['faster']

file_name = "web_csv_files/big.csv"
file_url = "http://35.241.167.153/big.csv"

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

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

# attempts:

results_file = open('results.txt', 'a')

localid = 't'

while localid != 'x':
    localid = input('\nEnter localid or x to exit: \n')
    time_before_running = datetime.datetime.now()
    print(find_address(localid))
    print('mongo: ', datetime.datetime.now() - time_before_running)
    results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

results_file.write(f'\n**********************\n')


results_file.close()


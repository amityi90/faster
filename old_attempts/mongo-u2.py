import csv
import math
import datetime
import os
from pymongo import MongoClient
import urllib.request
import re
from decouple import config

client = MongoClient()
db = client['faster']
# collection = db['big']




file_name = "web_csv_files/big.csv"
file_url = "http://35.241.167.153/big.csv"

# -----------------------------------
# push all to one 'big' collection

# urllib.request.urlretrieve(file_url, file_name)
# rows = []
# with open(file_name, 'r', encoding="utf8") as file:
#     csvreader = csv.reader(file)
#     for i, row in enumerate(csvreader):
#         address = { 
#             'index' : row[0],
#             'localid' : row[1],
#             'table_name' : row[2],
#             'fulladdress' : row[3],
#             'city_id' : row[4],
#         }
#         collection.insert_one(address).inserted_id
#         print(row, '\n', i, '\n--------------\n')

# time_before_running = datetime.datetime.now()
# print(collection.find_one({"localid": "1397003TK7519N"}))
# print('mongo: ', datetime.datetime.now() - time_before_running)

# -----------------------------------

# pushing to multiple collections

def download_csv():
    urllib.request.urlretrieve(file_url, file_name)

def push_csv_to_db():
    with open(file_name, 'r', encoding="utf8") as file:
        csvreader = csv.reader(file)
        for i, row in enumerate(csvreader):
            collection_name = ''
            if i == 0:
                continue
            address = { 
                'index' : row[0],
                'localid' : row[1],
                'table_name' : row[2],
                'fulladdress' : row[3],
                'city_id' : row[4],
            }
            if re.match(r"[0-9]+[A-Z][A-Z]?[0-9]+[A-Z][A-Z]?", address['localid']):
                letters_index = re.search(r"[A-Z][A-Z]?", address['localid'])
                letters_index = letters_index.span()[0]
                collection_name = address['localid'][letters_index:]
            elif re.match(r"[0-9]+[A-Z][A-Z]?[0-9]+", address['localid']):
                letters_index = re.search(r"[A-Z][A-Z]?", address['localid'])
                letters_index = letters_index.span()[1]
                collection_name = address['localid'][:letters_index]
            else:
                collection_name = address['localid'][:3]
            print(row, f'\n#{i}\n', '\n', collection_name)
            collection = db[collection_name]
            collection.insert_one(address).inserted_id
            print('\n--------------\n')


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

def find_address_from_big(address):
    collection = db['big']
    return collection.find_one({"localid": address})

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

# attempts:

# push_csv_to_db()

results_file = open('results.txt', 'a')


# time_before_running = datetime.datetime.now()
# print(find_address_from_big("14038A00900312"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address_from_big("14038A00900313"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address_from_big("14038A00900300"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address_from_big("14038A00900298"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address_from_big("14038A00900313"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# # ^^^^^^^^^^^^^^^^^^^^^^^

# time_before_running = datetime.datetime.now()
# print(find_address("0565106VK4706F"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address("11005A01900057"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address("21DS00003183AA"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address("02129E4VK4801A"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address("02129E4VK4801A"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address("14038A00900313"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')



# time_before_running = datetime.datetime.now()
# print(find_address("1243604VK4184D"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


# time_before_running = datetime.datetime.now()
# print(find_address("14038A00900312"))
# print('mongo: ', datetime.datetime.now() - time_before_running)
# results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


time_before_running = datetime.datetime.now()
print(find_address("24147A10105523"))
print('mongo: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

results_file.write(f'\n**********************\n')


results_file.close()








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
#         print(row, '\n', cluster_name)
#         # collection.insert_one(address).inserted_id
#         # print(row, '\n--------------\n')

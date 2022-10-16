import chunk
import csv
import math
import datetime
import os
from pymongo import MongoClient
import urllib.request
import re
from multiprocessing import Process
import shelve
from decouple import config


client = MongoClient()
db = client['faster']
db_file_name = config('SHELVE_DB_FILE_NAME')
print(db_file_name)


def push_chunk_to_db(lines):
        for i, row in enumerate(lines):
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
            print(row, f'\n#{i} -- in mongo\n', '\n', collection_name)
            collection = db[collection_name]
            collection.insert_one(address).inserted_id
            print('\n--------------\n')

def push_chunk_to_shelve(lines):
    d = shelve.open(db_file_name, 'c') 
    for i, row in enumerate(lines):
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
        key = address['localid']
        d[key] = address
        print(row, f'\n#{i} -- in shelve\n', '\n', collection_name)
        print('\n--------------\n')
    d.close()


class FastExtractor:

    def __init__(self, csv_url):
        self.csv_url = csv_url
        self.client = MongoClient()
        self.db = self.client['faster']
        self.csv_file_name = config('CSV_URL')
        self.results_file = open('results.txt', 'a')
        self.processes = []
        self.lines = []


    def download_csv(self):
        urllib.request.urlretrieve(self.csv_url, self.csv_file_name)

    def push_csv_to_db_mp(self):
        with open(self.csv_file_name, 'r', encoding="utf8") as file:
            csvreader = csv.reader(file)
            number_of_processes = 100
            self.lines = list(csvreader)
            print(len(self.lines))
            index = 1
            step = int(len(self.lines) / number_of_processes) + len(self.lines) % number_of_processes
            for i in range(number_of_processes):
                doc_chunk = self.lines[index:index + step]
                process = Process(target=push_chunk_to_db, args=([doc_chunk]))
                process.start()
                index += step
                self.processes.append(process)
    
    def push_csv_to_shelve_mp(self):
        with open(self.csv_file_name, 'r', encoding="utf8") as file:
            csvreader = csv.reader(file)
            number_of_processes = 100
            self.lines = list(csvreader)
            print(len(self.lines))
            index = 1
            step = int(len(self.lines) / number_of_processes) + len(self.lines) % number_of_processes
            for i in range(number_of_processes):
                doc_chunk = self.lines[index:index + step]
                process = Process(target=push_chunk_to_shelve, args=([doc_chunk]))
                process.start()
                index += step
                self.processes.append(process) 

    


    def join_processes(self):
        for process in self.processes:
            process.join()

    def push_csv_to_db(self):
        with open(self.csv_file_name, 'r', encoding="utf8") as file:
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
                collection = self.db[collection_name]
                collection.insert_one(address).inserted_id
                print('\n--------------\n')

    def find_address(self, address):
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
        collection = self.db[collection_name]
        return collection.find_one({"localid": address})

    def find_address_shelve(self, address):
        d = shelve.open(db_file_name, 'r')  
        results_file = open('results.txt', 'a')
        time_before_running = datetime.datetime.now()
        data = d[address]
        print(data)
        print('shelve: ', datetime.datetime.now() - time_before_running)
        results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')
        results_file.write(f'\n**********************\n')
        results_file.close()
        d.close()    
    
    def close_res_doc(self):
        self.results_file.close()

    def extract_session(self):
        localid = 't'
        while localid != 'x':
            localid = input('\nEnter localid or x to exit: \n')
            time_before_running = datetime.datetime.now()
            print(self.find_address(localid))
            print('extraction time: ', datetime.datetime.now() - time_before_running)
            self.results_file.write(f'at: {datetime.datetime.now()} | extraction time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

        self.results_file.write(f'\n**********************\n')

if __name__ == '__main__':
    time_before_running = datetime.datetime.now()
    addres_session = FastExtractor("http://35.241.167.153/small.csv")
    # addres_session.push_csv_to_shelve_mp()
    # addres_session.join_processes()
    try:
        addres_session.find_address_shelve(config('LOCAL_ID'))
    except:
        pass
    try:
        print(addres_session.find_address(config('LOCAL_ID')))
    except:
        pass



    print(f'\n\n     ----- that took: {datetime.datetime.now() - time_before_running}-----\n\n     cho')


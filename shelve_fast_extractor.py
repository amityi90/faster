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



class FastExtractor:

    def __init__(self, csv_url):
        self.csv_url = csv_url
        self.db_file_name = config('SHELVE_DB_FILE_NAME')
        self.csv_file_name = config('CSV_URL')
        self.results_file = open('results.txt', 'a')
        self.processes = []
        self.lines = []


    def download_csv(self):
        urllib.request.urlretrieve(self.csv_url, self.csv_file_name)
        
    def push_csv_to_shelve(self):
        with open(self.csv_file_name, 'r', encoding="utf8") as file:
            csvreader = csv.reader(file)
            d = shelve.open(self.db_file_name, 'c') 
            for i, row in enumerate(csvreader):
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
                print(row, f'\n\n#{i} -- in shelve\n\n')
                print('\n--------------\n')
            d.close()

    def find_address_shelve(self, address):
        d = shelve.open(self.db_file_name, 'r')  
        results_file = open('results.txt', 'a')
        time_before_running = datetime.datetime.now()
        data = d[address]
        print(data)
        print('\nextraction time: ', datetime.datetime.now() - time_before_running)
        results_file.write(f'at: {datetime.datetime.now()} | extraction time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')
        results_file.write(f'\n**********************\n')
        results_file.close()
        d.close()    
    
    def close_res_doc(self):
        self.results_file.close()

    def extract_session(self):
        localid = 't'
        while localid != 'x':
            localid = input('\nEnter localid or x to exit: \n')
            if localid != 'x':
                self.find_address_shelve(localid)

if __name__ == '__main__':
    addres_session = FastExtractor("http://35.241.167.153/big.csv")
    # addres_session.push_csv_to_shelve()
    # addres_session.join_processes()
    # addres_session.push_csv_to_db()
    # addres_session.join_processes()
    addres_session.extract_session()



    print(f'\n\n     ----- -----\n\n     cho')


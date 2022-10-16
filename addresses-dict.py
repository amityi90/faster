import shelve
import csv
import os
import datetime
import urllib.request



db_file_name = 'big'
source_file_name = "web_csv_files\\big.csv"

def push_csv_to_db():
    d = shelve.open(db_file_name, 'c')      
    with open(source_file_name, 'r', encoding="utf8") as file:
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
            key = address['localid']
            d[key] = address
            print(row, f'\n#{i}\n', '\n', collection_name)
            print('\n--------------\n')
    d.close()             



#  push_csv_to_db()


d = shelve.open(db_file_name, 'r')  
results_file = open('results.txt', 'a')


time_before_running = datetime.datetime.now()
print(d['0673202VK4707D'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['0610507VK4701B'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['02129E4VK4801A'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['14038A00900313'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['0673202VK4707D'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['1243604VK4184D'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['14038A00900312'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['0565106VK4706F'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['21DS00003183AA'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')

time_before_running = datetime.datetime.now()
print(d['11005A01900057'])
print('shelve: ', datetime.datetime.now() - time_before_running)
results_file.write(f'at: {datetime.datetime.now()} big search time: {datetime.datetime.now() - time_before_running}\n--------------------------------\n')


results_file.write(f'\n**********************\n')


results_file.close()
d.close()             
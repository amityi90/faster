import csv
from venv import create
import pandas as pd
import math
import datetime
import os
import mmap


file_name = "csv_files\\big.csv"
# iterate all rows:

# rows = []
# with open(file_name, 'r', encoding="utf8") as file:
#     csvreader = csv.reader(file)
#     found_row  = []
#     time_before_running = datetime.datetime.now()
#     for row in csvreader:
#         if '000205600VK56E' in row:
#             print(row)
#             found_row = row
#             break
#         print(row, '\n--------------\n')
#         print(found_row)
#         os.system('cls')

        
#     print('iterate all rows: ', datetime.datetime.now() - time_before_running)


# # # binar search:

# def binar_search(df, target):
#     small_index = 0
#     big_index = df.shape[0] - 1
#     while small_index <= big_index:
#         middle_index = math.floor((small_index + big_index) / 2)
#         value = str(df.iloc[middle_index, 0])
#         if value < target:
#             small_index = middle_index + 1
#         elif value > target:
#             big_index = middle_index - 1
#         elif value == target:
#             return df.iloc[[middle_index]]
#     return 'could not find'



# data= pd.read_csv(file_name)
# data = data.sort_values(by=['localid'], ignore_index=True)
# time_before_running = datetime.datetime.now()
# print(binar_search(data, '9803532VK4890D'))
# print('binar search: ', datetime.datetime.now() - time_before_running)

# # # string in:
 
# data= pd.read_csv(file_name)
# data = data.to_string()
# print('searching...')
# time_before_running = datetime.datetime.now()
# print(data.index('9803532VK4890D'))
# print('string index search: ', datetime.datetime.now() - time_before_running)

# # dict style:

# def dict_style(df, target):
#     new_dict = {}
#     for i in range(df.shape[0] - 1):
#         new_dict[f'{df.iloc[i, 0]}'] = df.iloc[i]
#     print('searching...')
#     time_before_running = datetime.datetime.now()
#     print(new_dict[target])
#     print('dict style: ', datetime.datetime.now() - time_before_running)
#     print(len(new_dict))


# data= pd.read_csv(file_name)
# dict_style(data, '9803532VK4890D')

# dict dict dict:

# def dict_style(df, target):
#     new_dict = {}
#     def check_level(dict, str, level):
#         if dict[str[:3 * level]]:
#             return check_level(dict[str[:3 * level]], str, level + 1)
#         else:
#             return level
    
#     def create_sub_dict(dict, str, level):
#         if level * 3 > 14:
#             return dict
#         dict[str[:level * 3]] = create(dict, str, level + 1)


    
#     def make_level(str, level):
#         sub_dict = {}
#         while level:
#             sub_dict[sub_dict]
    
#     # counter = 5
#     # while counter:
#     #     if


#     new_dict = {}
#     for i in range(df.shape[0] - 1):
#         if new_dict[f'{df.iloc[i, 0][:2]}']:
#         new_dict[f'{df.iloc[i, 0]}'] = df.iloc[i]


    
#     print('searching...')
#     time_before_running = datetime.datetime.now()
#     print(new_dict[target])
#     print('dict style: ', datetime.datetime.now() - time_before_running)
#     print(len(new_dict))



with open(file_name, 'r+b') as file:
    mm = mmap.mmap(file.fileno(), 0)
    time_before_running = datetime.datetime.now()
    print(mm.find(b'9803532VK4890D'))
    print('mmap search: ', datetime.datetime.now() - time_before_running)
    mm.close()


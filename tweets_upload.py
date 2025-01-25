import json
import os
from pathlib import Path
import re
import pandas as pd
from tqdm import tqdm
from p_tqdm import p_umap
import time
from dotenv import load_dotenv
from pymongo import MongoClient
import multiprocessing
import dateparser
from os import listdir


#f = open('/home/mlp2/ukraine_tweets/ukraine03012022e.json')
#data = json.load(f)
#file = '/home/mlp2/ukraine_tweets/ukraine03012022e.json'
folder_files = [f for f in listdir('/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/')]
files = [f for f in folder_files if '.json' in f]
#files = [f for f in files if f not in ['ukraine02282022a.json']]

db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

for file in files:
    for line in open(file, 'r'):
        #tweets.append(json.loads(line))#
        
        dic = json.loads(line)

        date1 = dic['created_at']
        date2 = date1 [-4:]+' '+ date1 [4:-10]
        date_created = dateparser.parse(date2).replace(tzinfo = None) 
        

        dic["created_at"] = date2
        dic["keyword"] = "російський" ################################## remember to check the keyword is corresponding to what you collected!

        colname = f'tweets-{date_created.year}-{date_created.month}-{date_created.day}'

        l = [j for j in db[colname].find({'id': dic['id']} )]
        if l ==[]:
            db[colname].insert_one(dic)
            print(f'{file}: uploading to {colname}')
        else:
            db[colname].update_one({'id': dic['id']},{'$set': {"keyword": "ukraine"}})
            print(f'{file}: updating ', colname)
            pass

print('Done: ',files)




    






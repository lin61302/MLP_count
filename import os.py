import os
from pathlib import Path
import re
import pandas as pd
from tqdm import tqdm
from p_tqdm import p_umap
import time
from dotenv import load_dotenv
from pymongo import MongoClient

today = pd.Timestamp.now()
load_dotenv()
#uri = os.getenv('DATABASE_URL')
uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
db = MongoClient(uri).ml4p

__russiapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Acronyms_Russia_Test_4.xlsx'
__chinapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Acronyms_China_Test_4.xlsx'

russia = pd.read_excel(__russiapath__)
china = pd.read_excel(__chinapath__)
ru = russia['CompanyName'].str.strip()
ch = china['CompanyName'].str.strip()
ru_ind = russia['alphabet_connect']
ch_ind = china['alphabet_connect']
for i, doc in enumerate(ru):
    if bool(int(ru_ind[i])):
        ru[i] = ru[i][2:-2].rstrip().lstrip()
    else:
        ru[i] = ru[i][2:-2].rstrip().lstrip()
        # ru[i] = "(?<![a-z])" + ru[i][2:-2].rstrip().lstrip() + "(?![a-z])"
for i, doc in enumerate(ch):
    if bool(int(ch_ind[i])):
        ch[i] = ch[i][2:-2].rstrip().lstrip()
    else:
        ch[i] = ch[i][2:-2].rstrip().lstrip()
        # ch[i] = "(?<![a-z])" + ch[i][2:-2].rstrip().lstrip() + "(?![a-z])"
ru_1 = '|'.join(ru)
ch_1 = '|'.join(ch)
print(ru_1)
print('\n\n\n')
print(ch_1)


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

load_dotenv()
#uri = os.getenv('DATABASE_URL')
uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
db = MongoClient(uri).ml4p

today = pd.Timestamp.now()

events = [k for k in db.models.find_one({'model_name': 'civic1'}).get('event_type_nums').keys()] + ['-999']

legal_re = re.compile(r'(freedom.*|assembl.*|associat.*|term limit.*|independen.*|succession.*|demonstrat.*|crackdown.*|draconian|censor.*|authoritarian|repress.*|NGO|human rights)')

censor_re = re.compile(r'(journal.*|newspaper|media|outlet|censor|reporter|broadcast.*|correspondent|press|magazine|paper|black out|blacklist|suppress|speaking|false news|fake news|radio|commentator|blogger|opposition voice|voice of the opposition|speech|broadcast|publish)')

defame_re1 = re.compile(r'(case|lawsuit|sue|suit|trial)')
defame_re2 = re.compile(r'(defamation|defame|libel|slander|insult|reputation)')

def check_legal(doc):
    try:
        if bool(legal_re.search(doc.get('title_translated'))) or bool(legal_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False

def check_censorship(doc):
    try:
        if bool(censor_re.search(doc.get('title_translated'))) or bool(censor_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False

def check_defamation(doc):
    try:
        if bool(defame_re1.search(doc.get('title_translated'))) or bool(defame_re1.search(doc.get('maintext_translated'))) and bool(defame_re2.search(doc.get('title_translated'))) or bool(defame_re2.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False

def update_info(docs, event_types, colname):
    """
    updates the docs into the db
    """
    db = MongoClient(uri).ml4p

    
    for nn, _doc in enumerate(docs):
        colname = "Temporary_Pipeline"
        db[colname].update_one(
            {
                'uuid': _doc['uuid']
            },
            {
                '$set':{
                    'event_type_civic1':event_types[nn]
                            
                }
            }
        )

# START WITH THE LOCALS
def keyword_filter(uri):

    db = MongoClient(uri).ml4p
    
    colname = 'Temporary_Pipeline'

    cur = db[colname].find(
            {  
                'civic1': {'$exists': True}
            }
        )
    docs = [doc for doc in cur]

        #original event type
    event_types = [doc['civic1']['event_type'] for doc in docs]
        #update event type based on filter outcome
    for index, doc in enumerate(docs):

        if doc['civic1']['event_type'] == 'legalaction' and check_defamation(doc):
            event_types[index] = 'defamationcase'

        elif doc['civic1']['event_type'] == 'legalchange':
            if check_legal(doc):
                    event_types[index] = 'legalchange'
            else: 
                event_types[index] = '-999'

        elif doc['civic1']['event_type'] == 'censor':
            if check_censorship(doc):
                event_types[index] = 'censor'
            else: 
                event_types[index] = '-999'

        else:
            event_types[index] = doc['civic1']['event_type']

    print('Filtered!')

        #update data with new event_types
    proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, colname = colname))
    proc.start()
    print('Updated!')

            
keyword_filter(uri=uri)

 

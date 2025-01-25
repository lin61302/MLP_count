#!/usr/bin/env python3
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

today = pd.Timestamp.now()
load_dotenv()
#uri = os.getenv('DATABASE_URL')
uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
db = MongoClient(uri).ml4p

__russiapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Acronyms_Russia_Test_3.xlsx'
__chinapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Acronyms_China_Test_3.xlsx'
#__chinaspanishpath__ = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/" + os.getenv('ACRONYMS_CHINA_SPANISH')
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

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
        ru[i] = "(?<![a-z])" + ru[i][2:-2].rstrip().lstrip() + "(?![a-z])"
for i, doc in enumerate(ch):
    if bool(int(ch_ind[i])):
        ch[i] = ch[i][2:-2].rstrip().lstrip()
    else:
        ch[i] = "(?<![a-z])" + ch[i][2:-2].rstrip().lstrip() + "(?![a-z])"
ru_ch = '|'.join(pd.concat([ru, ch]))
rai_re = re.compile(ru_ch)


events = [k for k in db.models.find_one({'model_name': 'RAI'}).get('event_type_nums').keys()]

geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip()
g_loc = geo_loc['CompanyName'].str.strip()
for i, doc in enumerate(g_int):
    g_int[i] = "(?<![a-z])" + g_int[i][2:-2].rstrip().lstrip() + "(?![a-z])"
for i, doc in enumerate(g_loc):
    g_loc[i] = "(?<![a-z])" + g_loc[i][2:-2].rstrip().lstrip() + "(?![a-z])"
g_int_string = '|'.join(g_int)
g_loc_string = '|'.join(g_loc)
g_int_filter = re.compile(g_int_string,flags=re.IGNORECASE)
g_loc_filter = re.compile(g_loc_string,flags=re.IGNORECASE)



def check_georgia(doc, _domain):
    if _domain == 'loc':
        try:
            if bool(g_loc_filter.search(doc)):
                return False
            else:
                return True
        except:
            return True
    if _domain == 'int':
        try:
            if bool(g_int_filter.search(doc)):
                return False
            else:
                return True
        except:
            return True

def check_rai(doc):   
    try:
        if bool(rai_re.search(doc)):
            return True
        else:
            return False
    except:
        return False


events = [k for k in db.models.find_one({'model_name': 'RAI'}).get('event_type_nums').keys()]



def update_info(docs, event_types, colname):
    """
    updates the docs into the db
    """
    db = MongoClient(uri).ml4p

    for nn, _doc in enumerate(docs):
        try:
            colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        except:
            dd = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
            colname = f"articles-{dd.year}-{dd.month}"
            
        db[colname].update_one(
            {
                '_id': _doc['_id']
            },
            {
                '$set':{
                    'event_type_RAI':event_types[nn]
                            
                }
            }
        )

# START WITH THE LOCALS
def count_domain_loc(uri, domain, country_name, country_code):
    db = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]

    for et in events:
        df[et] = [0] * len(df)

    for date in df.index:
        
        colname = f'articles-{date.year}-{date.month}'
        

        cur = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True}, 
                'maintext_translated':{'$exists':True},
                'title_translated':{'$exists':True},
                
                '$or': [
                        {'mordecai_locations.' + country_code : {'$exists' : True}},
                        {'mordecai_locations' : {}}
                    ]
                
            }
        )
        docs = [doc for doc in cur]
        event_types = []
        #check rai and update event type
        for index, doc in enumerate(docs):

            try:
                if check_rai(docs[index]['maintext_translated']) or check_rai(docs[index]['title_translated']):
                    event_types.append(doc['RAI']['event_type'])

                else:
                    event_types.append('-999')
            except:
                event_types.append('-999')

        #update data with new event_types
        proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, colname = colname))
        proc.start()

        for et in events:

            sub_docs = [doc for doc in docs if doc['RAI']['event_type']==et]
            sub_docs = [doc for doc in sub_docs if check_rai(doc['maintext_translated']) or check_rai(doc['title_translated'])]
            if country_code == 'GEO':
                sub_docs = [doc for doc in sub_docs if check_georgia(doc['maintext_translated'], _domain='loc') and check_georgia(doc['title_translated'], _domain='loc')]
            count = len(sub_docs)

            df.loc[date, et] = count      
        if country_code == 'GEO':
            for nn, _doc in enumerate(docs):
                    try:
                        colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                    except:
                        dd = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
                        colname = f"articles-{dd.year}-{dd.month}"
                    if check_georgia(_doc['maintext_translated'], _domain='loc') and check_georgia(_doc['title_translated'], _domain='loc'):
                        db[colname].update_one(
                            {
                                '_id': _doc['_id']
                            },
                            {
                                '$set':{
                                    'Country_Georgia': 'Yes'
                                            
                                }
                            }
                            )
                    else:
                        db[colname].update_one(
                            {
                                '_id': _doc['_id']
                            },
                            {
                                '$set':{
                                    'Country_Georgia': 'No'
                                            
                                }
                            }
                            )

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/(mordecai){today.year}_{today.month}_{today.day}/'
    
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    df.to_csv(path + f'{domain}.csv')



# Then ints
def count_domain_int(uri, domain, country_name, country_code):
    
    db = MongoClient(uri).ml4p
    
    # create a new frame to work with
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]

    for et in events:
        df[et] = [0] * len(df)

    for date in df.index:
        colname = f'articles-{date.year}-{date.month}'

        cur = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True},
                'mordecai_locations.' + country_code : {'$exists' : True}           
            }
        )
        docs = [doc for doc in cur]
        event_types = []
        #check rai and update event type
        for index, doc in enumerate(docs):

            try:
                if check_rai(docs[index]['maintext_translated']) or check_rai(docs[index]['title_translated']):
                    event_types.append(doc['RAI']['event_type'])

                else:
                    event_types.append('-999')
            except:
                event_types.append('-999')
                
        #update data with new event_types
        proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, colname = colname))
        proc.start()

        for et in events:

            sub_docs = [doc for doc in docs if doc['RAI']['event_type']==et]
            sub_docs = [doc for doc in sub_docs if check_rai(doc['maintext_translated']) or check_rai(doc['title_translated'])]
            if country_code == 'GEO':
                sub_docs = [doc for doc in sub_docs if check_georgia(doc['maintext_translated'], _domain='int') and check_georgia(doc['title_translated'], _domain='int')]
            count = len(sub_docs)

            df.loc[date, et] = count

        if country_code == 'GEO':
            for nn, _doc in enumerate(docs):
                    try:
                        colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                    except:
                        dd = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
                        colname = f"articles-{dd.year}-{dd.month}"
                    if check_georgia(_doc['maintext_translated'], _domain='int') and check_georgia(_doc['title_translated'], _domain='int'):
                        db[colname].update_one(
                            {
                                '_id': _doc['_id']
                            },
                            {
                                '$set':{
                                    'Country_Georgia': 'Yes'
                                            
                                }
                            }
                            )
                    else:
                        db[colname].update_one(
                            {
                                '_id': _doc['_id']
                            },
                            {
                                '$set':{
                                    'Country_Georgia': 'No'
                                            
                                }
                            }
                            )
        
        

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/(mordecai){today.year}_{today.month}_{today.day}/'
    
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    df.to_csv(path + f'{domain}.csv')

if __name__ == "__main__":
    
    countries = [
        # ('Albania', 'ALB'), 
        # ('Benin', 'BEN'),
        # ('Colombia', 'COL'),
        # ('Ecuador', 'ECU'),
        # ('Ethiopia', 'ETH'),
        # ('Georgia', 'GEO'),
        ('Kenya', 'KEN'),
        # ('Paraguay', 'PRY'),
        # ('Mali', 'MLI'),
        # ('Morocco', 'MAR'),
        #('Nigeria', 'NGA'),
        # ('Serbia', 'SRB'),
        # ('Senegal', 'SEN'),
        # ('Tanzania', 'TZA'),
        # ('Uganda', 'UGA'),
        #('Ukraine', 'UKR'),
        # ('Zimbabwe', 'ZWE'),
        # ('Mauritania', 'MRT'),
        # ('Zambia', 'ZMB'),
        #('Kosovo', 'XKX')
        # ('Niger', 'NER'),
        # ('Jamaica', 'JAM'),
        # ('Honduras', 'HND'),
        # ('Philippines', 'PHL'),
        # ('Ghana', 'GHA'),
        # ('Rwanda','RWA'),
        # ('Guatemala','GTM')
        # ('Ecuador', 'ECU')
        # ('Belarus','BLR')
        # ('Congo','COD')
        # ('Cambodia','KHM')
    ]


    for ctup in countries:

        print('Starting: '+ ctup[0])

        country_name = ctup[0]
        country_code = ctup[1]

        loc = [doc['source_domain'] for doc in db['sources'].find(
            {
                'primary_location': {'$in': [country_code]},
                'include': True
            }
        )]

        ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
        regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True
        , 'include': True})]
        
        p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)
        p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=10)
        p_umap(count_domain_int, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=10)
 
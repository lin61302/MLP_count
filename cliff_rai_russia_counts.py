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
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

__russiapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Russia_RAI_keywords_0730.xlsx'
#__chinapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Acronyms_China_Test_3.xlsx'
#__chinaspanishpath__ = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/" + os.getenv('ACRONYMS_CHINA_SPANISH')
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

russia = pd.read_excel(__russiapath__)
#china = pd.read_excel(__chinapath__)
ru = russia['CompanyName'].str.strip()
#ch = china['CompanyName'].str.strip()
ru_ind = russia['alphabet_connect']
#ch_ind = china['alphabet_connect']
for i, doc in enumerate(ru):
    if bool(int(ru_ind[i])):
        ru[i] = ru[i][2:-2].rstrip().lstrip()
    else:
        ru[i] = "(?<![a-zA-Z])" + ru[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"
# for i, doc in enumerate(ch):
#     if bool(int(ch_ind[i])):
#         ch[i] = ch[i][2:-2].rstrip().lstrip()
#     else:
#         ch[i] = "(?<![a-z])" + ch[i][2:-2].rstrip().lstrip() + "(?![a-z])"
ru_ch = '|'.join(ru)
rai_re = re.compile(ru_ch)

title_re = re.compile(r'(russia|russian)',flags=re.IGNORECASE)


events = [k for k in db.models.find_one({'model_name': 'RAI'}).get('event_type_nums').keys()]

geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip()
g_loc = geo_loc['CompanyName'].str.strip()
for i, doc in enumerate(g_int):
    g_int[i] = "(?<![a-zA-Z])" + g_int[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"
for i, doc in enumerate(g_loc):
    g_loc[i] = "(?<![a-zA-Z])" + g_loc[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"
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
    
def check_title(doc):
    try:
        if bool(title_re.search(doc)):
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
                    'event_type_RAI_Russia':event_types[nn]
                            
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
        

        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True, '$ne': None,},
                # 'maintext_translated':{'$exists':True},
                # 'title_translated':{'$exists':True},
                'title_translated': {
                        '$exists': True,
                        '$ne': '',
                        '$ne': None,
                        '$type': 'string'
                    },
                'maintext_translated': {
                        '$exists': True,
                        '$ne': '',
                        '$ne': None,
                        '$type': 'string'
                    },
                'language': {'$ne': 'en'},
                
                '$or': [
                        {'cliff_locations.' + country_code : {'$exists' : True}},
                        {'cliff_locations' : {}}
                    ]
                
            }, batch_size=1
        )
        docs1 = [doc for doc in cur1]

        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True, '$ne': None,},
                # 'maintext_translated':{'$exists':True},
                # 'title_translated':{'$exists':True},
                'title_translated': {
                        '$exists': True,
                        '$ne': '',
                        '$ne': None,
                        '$type': 'string'
                    },
                'maintext_translated': {
                        '$exists': True,
                        '$ne': '',
                        '$ne': None,
                        '$type': 'string'
                    },
                'language': 'en'  ,
                '$or': [
                    {'en_cliff_locations.' + country_code : {'$exists' : True}},
                    {'en_cliff_locations' : {}}
                ]
                
            }, batch_size=1
        )
        docs2 = [doc for doc in cur2]
        docs = docs1+docs2
        
        event_types, keywords = [], [[] for _ in range(len(docs))]

        # Step 1: Process documents and filter by keywords (including GEO-specific filtering)
        for index, doc in enumerate(docs):
            # Apply keyword checks (RAI and title)
            if check_rai(doc['maintext_translated'][:2000]) or check_rai(doc['title_translated']) or check_title(doc['title_translated']):
                event_types.append(doc['RAI']['event_type'])
                text = f"{doc['title_translated']} {doc['maintext_translated'][:2000]}"
                matches = rai_re.findall(text)
                keywords[index].extend(matches)
            else:
                event_types.append('-999')

        # Step 2: Remove duplicates from keyword lists
        keywords = [list(set(k)) for k in keywords]

        # Step 3: Update database with filtered event types and keywords
        proc = multiprocessing.Process(target=update_info(docs, event_types, colname), args=(docs, event_types, keywords))
        proc.start()
        proc.join() 

        # Step 4: Filter and count events for each event type (applying GEO-specific checks)
        for et in events:
            if et == '-999':
                # Directly count documents where event_types[index] is '-999'
                count = sum(1 for index, doc in enumerate(docs) if event_types[index] == '-999')
            else:
                # Directly count documents where both RAI and event_types match the event type
                count = sum(1 for index, doc in enumerate(docs) if doc['RAI'].get('event_type') == et and event_types[index] == et)

            if country_code == 'GEO':
                # Function to handle the GEO filtering for both cases
                def apply_int_geo_filter(index, doc):
                    return check_georgia(doc['maintext_translated'][:2000], _domain='loc') and check_georgia(doc['title_translated'], _domain='loc')

                count = sum(
                    1 for index, doc in enumerate(docs)
                    if (event_types[index] == '-999' if et == '-999' else doc['RAI'].get('event_type') == et and event_types[index] == et)
                    and apply_int_geo_filter(index, doc)
                )
            
            # Store the count in the DataFrame
            df.loc[date, et] = count 

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Russia/'
    
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
                'RAI': {'$exists': True, '$ne': None,},
                'language': {'$ne': 'en'},
                'cliff_locations.' + country_code : {'$exists' : True}, 

            }
        )
        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True, '$ne': None,},
                'language': 'en' ,
                'en_cliff_locations.' + country_code : {'$exists' : True},
                         
            }
        )
        docs1 = [doc for doc in cur]
        docs2 = [doc for doc in cur2]
        docs = docs1+docs2
        event_types, keywords = [], [[] for _ in range(len(docs))]

        # Step 1: Process documents and filter by keywords (including GEO-specific filtering)
        for index, doc in enumerate(docs):
            # Apply keyword checks (RAI and title)
            if check_rai(doc['maintext_translated'][:2000]) or check_rai(doc['title_translated']) or check_title(doc['title_translated']):
                event_types.append(doc['RAI']['event_type'])
                text = f"{doc['title_translated']} {doc['maintext_translated'][:2000]}"
                matches = rai_re.findall(text)
                keywords[index].extend(matches)
            else:
                event_types.append('-999')

        # Step 2: Remove duplicates from keyword lists
        keywords = [list(set(k)) for k in keywords]

        # Step 3: Update database with filtered event types and keywords
        proc = multiprocessing.Process(target=update_info(docs, event_types, colname), args=(docs, event_types, keywords))
        proc.start()
        proc.join() 

        # Step 4: Filter and count events for each event type (applying GEO-specific checks)
        for et in events:
            if et == '-999':
                # Directly count documents where event_types[index] is '-999'
                count = sum(1 for index, doc in enumerate(docs) if event_types[index] == '-999')
            else:
                # Directly count documents where both RAI and event_types match the event type
                count = sum(1 for index, doc in enumerate(docs) if doc['RAI'].get('event_type') == et and event_types[index] == et)

            if country_code == 'GEO':
                # Function to handle the GEO filtering for both cases
                def apply_int_geo_filter(index, doc):
                    return check_georgia(doc['maintext_translated'][:2000], _domain='int') and check_georgia(doc['title_translated'], _domain='int')

                count = sum(
                    1 for index, doc in enumerate(docs)
                    if (event_types[index] == '-999' if et == '-999' else doc['RAI'].get('event_type') == et and event_types[index] == et)
                    and apply_int_geo_filter(index, doc)
                )
            
            # Store the count in the DataFrame
            df.loc[date, et] = count 
        
        

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Russia/'
    
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    df.to_csv(path + f'{domain}.csv')

if __name__ == "__main__":
    
    slp = False
    # slp = True
    
    if slp:
        t = 7200
        print(f'start sleeping for {t/60} mins')
        time.sleep(t)
    
    countries = [
        # ('Albania', 'ALB'), 
        # ('Benin', 'BEN'),
        # ('Colombia', 'COL'),
        # ('Ecuador', 'ECU'),
        # ('Ethiopia', 'ETH'),
        # ('Georgia', 'GEO'),
        # ('Kenya', 'KEN'),
        # ('Paraguay', 'PRY'),
        # ('Mali', 'MLI'),
        # ('Morocco', 'MAR'),
        # ('Nigeria', 'NGA'),
        # ('Serbia', 'SRB'),
        # ('Senegal', 'SEN'),
        # ('Tanzania', 'TZA'),
        # ('Uganda', 'UGA'),
        # ('Ukraine', 'UKR'), 
        # ('Zimbabwe', 'ZWE'),
        # ('Mauritania', 'MRT'),
        # ('Zambia', 'ZMB'),
        # ('Kosovo', 'XKX'),
        # ('Niger', 'NER'),
        # ('Jamaica', 'JAM'),
        # ('Honduras', 'HND'),
        # ('Philippines', 'PHL'),
        # ('Ghana', 'GHA'),
        # ('Rwanda','RWA'),
        # ('Guatemala','GTM'),
        # ('Belarus','BLR'),
        # ('Cambodia','KHM'),
        # ('DR Congo','COD'),
        # ('Turkey','TUR'),
        # ('Bangladesh', 'BGD'),
        # ('El Salvador', 'SLV'),
        # ('South Africa', 'ZAF'),
        # ('Tunisia','TUN'),
        # ('Indonesia','IDN'),
        # ('Nicaragua','NIC'),
        # ('Angola','AGO'),
        # ('Armenia','ARM'), 
        # ('Sri Lanka', 'LKA'),
        # ('Malaysia','MYS'),
        # ('Cameroon','CMR'),
        # ('Hungary','HUN'),
        # ('Malawi','MWI'),
        # ('Uzbekistan','UZB'),
        ('India','IND'),
        # ('Mozambique','MOZ'),
        # ('Azerbaijan','AZE'),
        # ('Kyrgyzstan','KGZ'),
        # ('Moldova','MDA'),
        ('Kazakhstan','KAZ'),
        # ('Peru','PER'),
        # ('Algeria','DZA'),
        # ('Macedonia','MKD'), 
        # ('South Sudan','SSD'),
        # ('Liberia','LBR'),
        ('Pakistan','PAK'),
        # ('Nepal', 'NPL'),
        # ('Namibia','NAM'),
        # ('Burkina Faso', 'BFA'),
        # ('Dominican Republic', 'DOM'),
        # ('Timor Leste', 'TLS'),
        # ('Solomon Islands', 'SLB')
    ]

    # 1110


    for ctup in countries:

        print('Starting: '+ ctup[0])

        country_name = ctup[0]
        country_code = ctup[1]

        # loc=['elsalvador.com']

        if country_code == 'XKX':
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {
                    'primary_location': {'$in': [country_code]},
                    'include': True
                }
            )]+['balkaninsight.com']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True}) if doc['source_domain']!='balkaninsight.com']
        elif country_code == 'KAZ':
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {
                    'primary_location': {'$in': [country_code]},
                    'include': True
                }
            )]+['kaztag.kz']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]
            
        else:

            loc = [doc['source_domain'] for doc in db['sources'].find(
                {
                    'primary_location': {'$in': [country_code]},
                    'include': True
                }
            )]
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]
        # p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)
        # p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=10)
        # p_umap(count_domain_int, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=10)
        
        ind = 1
        while ind:
            try:
                p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)
                ind = 0
            except:
                pass
            
        ind = 1
        while ind:
            try:
                p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=10)
                ind = 0
            except:
                pass
        ind = 1
        while ind:
            try:
                p_umap(count_domain_int, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=10)
                ind = 0
            except:
                pass
 
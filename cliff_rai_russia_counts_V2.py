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
import subprocess

today = pd.Timestamp.now()
load_dotenv()
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p


__russiapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Russia_RAI_keywords_0730.xlsx'
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

russia = pd.read_excel(__russiapath__)
ru = russia['CompanyName'].str.strip()
ru_ind = russia['alphabet_connect']

# Build the Russia regex
for i, doc in enumerate(ru):
    if bool(int(ru_ind[i])):
        ru[i] = ru[i][2:-2].rstrip().lstrip()
    else:
        ru[i] = "(?<![a-zA-Z])" + ru[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"
ru_ch = '|'.join(ru)
rai_re = re.compile(ru_ch)

title_re = re.compile(r'(russia|russian)', flags=re.IGNORECASE)
events = list(db.models.find_one({'model_name': 'RAI'}).get('event_type_nums').keys())

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
g_int_filter = re.compile(g_int_string, flags=re.IGNORECASE)
g_loc_filter = re.compile(g_loc_string, flags=re.IGNORECASE)

def check_georgia(doc, _domain):
    try:
        if _domain == 'loc':
            return not g_loc_filter.search(doc)
        else:  # 'int'
            return not g_int_filter.search(doc)
    except:
        return True

def check_rai(doc):
    try:
        return bool(rai_re.search(doc))
    except:
        return False

def check_title(doc):
    try:
        return bool(title_re.search(doc))
    except:
        return False

def update_info(docs, event_types, colname):
    db_local = MongoClient(uri).ml4p
    for nn, _doc in enumerate(docs):
        try:
            actual_col = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        except:
            dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
            actual_col = f"articles-{dd.year}-{dd.month}"

        db_local[actual_col].update_one(
            {'_id': _doc['_id']},
            {'$set': {
                'event_type_RAI_Russia': event_types[nn]
            }}
        )

def count_domain_loc(uri, domain, country_name, country_code):
    db_local = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    for et in events:
        df[et] = 0

    # Projection for efficiency
    projection = {
        '_id': 1, 'RAI': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1
    }

    for date in df.index:
        colname = f'articles-{date.year}-{date.month}'

        # Non-English
        cur1 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True, '$ne': None},
                'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
                'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
                'language': {'$ne': 'en'},
                '$or': [
                    {f'cliff_locations.{country_code}': {'$exists': True}},
                    {'cliff_locations': {}}
                ]
            },
            projection=projection,
            batch_size=100
        )
        docs1 = list(cur1)

        # English
        cur2 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True, '$ne': None},
                'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
                'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
                'language': 'en',
                '$or': [
                    {f'en_cliff_locations.{country_code}': {'$exists': True}},
                    {'en_cliff_locations': {}}
                ]
            },
            projection=projection,
            batch_size=100
        )
        docs2 = list(cur2)
        docs = docs1 + docs2

        if not docs:
            continue

        event_types = []
        keywords = [[] for _ in range(len(docs))]

        # Step 1: doc-level checks
        for idx, doc in enumerate(docs):
            main_snip = doc['maintext_translated'][:2000]
            title_txt = doc['title_translated']
            if check_rai(main_snip) or check_rai(title_txt) or check_title(title_txt):
                event_types.append(doc['RAI']['event_type'])
                combined = f"{title_txt} {main_snip}"
                matches = rai_re.findall(combined)
                keywords[idx].extend(matches)
            else:
                event_types.append('-999')

        # Step 2: deduplicate
        keywords = [list(set(k)) for k in keywords]

        # Step 3: Update DB
        proc = multiprocessing.Process(target=update_info(docs, event_types, colname), args=(docs, event_types, keywords))
        proc.start()
        proc.join()

        # Step 4: count
        for et in events:
            if et == '-999':
                count_val = sum(1 for idx in range(len(docs)) if event_types[idx] == '-999')
            else:
                count_val = sum(1 for idx, d in enumerate(docs)
                                if d['RAI'].get('event_type') == et and event_types[idx] == et)

            if country_code == 'GEO' and count_val > 0:
                def apply_geo(i):
                    doc = docs[i]
                    assigned = (event_types[i] == et if et != '-999' else event_types[i] == '-999')
                    return assigned and check_georgia(doc['maintext_translated'][:2000], 'loc') and check_georgia(doc['title_translated'], 'loc')
                count_val = sum(apply_geo(i) for i in range(len(docs)))

            df.loc[date, et] = count_val

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Russia/'
    Path(path).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(path, f'{domain}.csv'))

def count_domain_int(uri, domain, country_name, country_code):
    db_local = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    for et in events:
        df[et] = 0

    projection = {
        '_id': 1, 'RAI': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1
    }

    for date in df.index:
        colname = f'articles-{date.year}-{date.month}'

        cur = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True, '$ne': None},
                'language': {'$ne': 'en'},
                f'cliff_locations.{country_code}': {'$exists': True}
            },
            projection=projection,
            batch_size=100
        )
        cur2 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI': {'$exists': True, '$ne': None},
                'language': 'en',
                f'en_cliff_locations.{country_code}': {'$exists': True}
            },
            projection=projection,
            batch_size=100
        )
        docs1 = list(cur)
        docs2 = list(cur2)
        docs = docs1 + docs2

        if not docs:
            continue

        event_types = []
        keywords = [[] for _ in range(len(docs))]

        for idx, doc in enumerate(docs):
            main_snip = doc['maintext_translated'][:2000]
            title_txt = doc['title_translated']
            if check_rai(main_snip) or check_rai(title_txt) or check_title(title_txt):
                event_types.append(doc['RAI']['event_type'])
                combined = f"{title_txt} {main_snip}"
                matches = rai_re.findall(combined)
                keywords[idx].extend(matches)
            else:
                event_types.append('-999')

        keywords = [list(set(k)) for k in keywords]

        proc = multiprocessing.Process(target=update_info(docs, event_types, colname), args=(docs, event_types, keywords))
        proc.start()
        proc.join()

        for et in events:
            if et == '-999':
                count_val = sum(1 for idx in range(len(docs)) if event_types[idx] == '-999')
            else:
                count_val = sum(1 for idx, d in enumerate(docs)
                                if d['RAI'].get('event_type') == et and event_types[idx] == et)

            if country_code == 'GEO' and count_val > 0:
                def apply_geo_int(i):
                    doc = docs[i]
                    assigned = (event_types[i] == et if et != '-999' else event_types[i] == '-999')
                    return assigned and check_georgia(doc['maintext_translated'][:2000], 'int') and check_georgia(doc['title_translated'], 'int')
                count_val = sum(apply_geo_int(i) for i in range(len(docs)))

            df.loc[date, et] = count_val

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Russia/'
    Path(path).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(path, f'{domain}.csv'))

def run_git_commands(commit_message):
    try:
        # Add only Python files using shell globbing
        subprocess.run("git add *.py", shell=True, check=True)
        # Commit changes with a message
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        # Push changes to the repository
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

if __name__ == "__main__":
    slp = False
    if slp:
        t = 7200
        print(f'Start sleeping for {t/60} mins')
        time.sleep(t)
    
    all_countries = [
        ('Albania', 'ALB'), 
        ('Benin', 'BEN'),
        ('Colombia', 'COL'),
        ('Ecuador', 'ECU'),
        ('Ethiopia', 'ETH'),
        ('Georgia', 'GEO'),
        ('Kenya', 'KEN'),
        ('Paraguay', 'PRY'),
        ('Mali', 'MLI'),
        ('Morocco', 'MAR'),
        ('Nigeria', 'NGA'),
        ('Serbia', 'SRB'),
        ('Senegal', 'SEN'),
        ('Tanzania', 'TZA'),
        ('Uganda', 'UGA'),
        ('Ukraine', 'UKR'), 
        ('Zimbabwe', 'ZWE'),
        ('Mauritania', 'MRT'),
        ('Zambia', 'ZMB'),
        ('Kosovo', 'XKX'),
        ('Niger', 'NER'),
        ('Jamaica', 'JAM'),
        ('Honduras', 'HND'),
        ('Philippines', 'PHL'),
        ('Ghana', 'GHA'),
        ('Rwanda','RWA'),
        ('Guatemala','GTM'),
        ('Belarus','BLR'),
        ('Cambodia','KHM'),
        ('DR Congo','COD'),
        ('Turkey','TUR'),
        ('Bangladesh', 'BGD'),
        ('El Salvador', 'SLV'),
        ('South Africa', 'ZAF'),
        ('Tunisia','TUN'),
        ('Indonesia','IDN'),
        ('Nicaragua','NIC'),
        ('Angola','AGO'),
        ('Armenia','ARM'), 
        ('Sri Lanka', 'LKA'),
        ('Malaysia','MYS'),
        ('Cameroon','CMR'),
        ('Hungary','HUN'),
        ('Malawi','MWI'),
        ('Uzbekistan','UZB'),
        ('India','IND'),
        ('Mozambique','MOZ'),
        ('Azerbaijan','AZE'),
        ('Kyrgyzstan','KGZ'),
        ('Moldova','MDA'),
        ('Kazakhstan','KAZ'),
        ('Peru','PER'),
        ('Algeria','DZA'),
        ('Macedonia','MKD'), 
        ('South Sudan','SSD'),
        ('Liberia','LBR'),
        ('Pakistan','PAK'),
        ('Nepal', 'NPL'),
        ('Namibia','NAM'),
        ('Burkina Faso', 'BFA'),
        ('Dominican Republic', 'DOM'),
        ('Timor Leste', 'TLS'),
        ('Solomon Islands', 'SLB'),
        ("Costa Rica",'CRI'),
        ('Panama','PAN')
    ]
    

    countries_needed = ['MAR','SSD','TZA','RWA','ZWE','COD','NER', 'TLS', 'GEO', 'PRY', 'ECU', 'MLI', 'JAM', 'KAZ' ,'ARM','MOZ']

    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

    for ctup in countries:
        print('Starting:', ctup[0])
        country_name = ctup[0]
        country_code = ctup[1]

        if country_code == 'XKX':
            loc = [doc['source_domain'] for doc in db['sources'].find({'primary_location': {'$in':[country_code]}, 'include': True})] + ['balkaninsight.com']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True}) if doc['source_domain']!='balkaninsight.com']
        elif country_code == 'KAZ':
            loc = [doc['source_domain'] for doc in db['sources'].find({'primary_location': {'$in':[country_code]}, 'include': True})]+['kaztag.kz']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]
        else:
            loc = [doc['source_domain'] for doc in db['sources'].find({'primary_location':{'$in':[country_code]}, 'include': True})]
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international':True, 'include':True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional':True, 'include':True})]

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

        # Git operations
        countries_added = '/'.join(countries_needed)
        commit_message = f"RAI Russia count ({countries_added}) update"
        # run_git_commands(commit_message)
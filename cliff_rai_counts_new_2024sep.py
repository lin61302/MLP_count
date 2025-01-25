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

# Load the environment variables and set up the database connection
load_dotenv()
today = pd.Timestamp.now()
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

# Paths to the new Excel files (replacing old paths)
__chinapath__ = '/mnt/data/China_RAI_keywords_0730.xlsx'
__russiapath__ = '/mnt/data/Russia_RAI_keywords_0730.xlsx'
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

# Load data from the Excel files
china = pd.read_excel(__chinapath__)
russia = pd.read_excel(__russiapath__)
geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)

# Strip any whitespace from the company names
ru = russia['CompanyName'].str.strip()
ch = china['CompanyName'].str.strip()
ru_ind = russia['alphabet_connect']
ch_ind = china['alphabet_connect']
g_int = geo_int['CompanyName'].str.strip()
g_loc = geo_loc['CompanyName'].str.strip()

# Compile the regular expressions for Russia, China, and Georgia filters
def compile_regex(keyword_list, boundary_list):
    regex_list = []
    for i, keyword in enumerate(keyword_list):
        if boundary_list[i]:  # Word boundary required
            regex_list.append(f"(?<![a-zA-Z]){re.escape(keyword)}(?![a-zA-Z])")
        else:
            regex_list.append(re.escape(keyword))
    return re.compile('|'.join(regex_list), flags=re.IGNORECASE)

rai_re = compile_regex(pd.concat([ru, ch]), pd.concat([ru_ind, ch_ind]))
g_int_filter = compile_regex(g_int, [True] * len(g_int))
g_loc_filter = compile_regex(g_loc, [True] * len(g_loc))

# Title filtering regex for 'china', 'chinese', 'russia', 'russian'
title_re = re.compile(r'(china|chinese|russia|russian)', flags=re.IGNORECASE)

# Check functions for different types of filtering
def check_georgia(doc, _domain):
    try:
        if _domain == 'loc':
            return not bool(g_loc_filter.search(doc))
        elif _domain == 'int':
            return not bool(g_int_filter.search(doc))
    except Exception:
        return True

def check_rai(doc):
    try:
        return bool(rai_re.search(doc))
    except Exception:
        return False

def check_title(doc):
    try:
        return bool(title_re.search(doc))
    except Exception:
        return False

# MongoDB update function (using bulk operations for efficiency)
def update_info(docs, event_types, keywords):
    db = MongoClient(uri).ml4p
    bulk_updates = []
    for nn, _doc in enumerate(docs):
        colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        bulk_updates.append({
            'filter': {'_id': _doc['_id']},
            'update': {
                '$set': {
                    'event_type_RAI': event_types[nn],
                    'RAI_keywords': keywords[nn]
                }
            }
        })
    if bulk_updates:
        db[colname].bulk_write([UpdateOne(**update) for update in bulk_updates])

# Main processing function for both local and international domains
def process_domain_counts(uri, domain, country_name, country_code, is_local=True):
    db = MongoClient(uri).ml4p
    df = pd.DataFrame(index=pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M'))
    df['year'] = df.index.year
    df['month'] = df.index.month
    events = [k for k in db.models.find_one({'model_name': 'RAI'}).get('event_type_nums').keys()]

    # Initialize event count columns
    for et in events:
        df[et] = 0

    for date in df.index:
        colname = f'articles-{date.year}-{date.month}'
        query = {
            'source_domain': domain,
            'include': True,
            'RAI': {'$exists': True},
            'language': {'$ne': 'en'}
        }
        if is_local:
            query[f'cliff_locations.{country_code}'] = {'$exists': True}
        else:
            query[f'en_cliff_locations.{country_code}'] = {'$exists': True}

        docs = list(db[colname].find(query))
        event_types, keywords = [], [[] for _ in range(len(docs))]

        for index, doc in enumerate(docs):
            if check_rai(doc['maintext_translated'][:2000]) or check_rai(doc['title_translated']) or check_title(doc['title_translated']):
                event_types.append(doc['RAI']['event_type'])
                text = f"{doc['title_translated']} {doc['maintext_translated'][:2000]}"
                matches = rai_re.findall(text)
                keywords[index].extend(matches)
            else:
                event_types.append('-999')

        keywords = [list(set(k)) for k in keywords]
        update_info(docs, event_types, keywords)

        # Count events for each event type
        for et in events:
            sub_docs = [doc for doc in docs if doc['RAI'].get('event_type') == et]
            df.loc[date, et] = len(sub_docs)

    # Save the results as CSV
    output_dir = f'/mnt/data/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(f'{output_dir}/{domain}.csv')

if __name__ == "__main__":
    slp = False
    if slp:
        t = 16200
        print(f'start sleeping for {t/60} mins')
        time.sleep(t)
    
    # Define countries to process
    countries = [
        ('Georgia', 'GEO'),
        ('Ukraine', 'UKR'),
        ('Philippines', 'PHL'),
        ('Turkey', 'TUR'),
        ('Armenia', 'ARM'),
        ('Malaysia', 'MYS'),
        ('Uzbekistan', 'UZB'),
        ('India', 'IND')
    ]

    for country in countries:
        country_name, country_code = country

        # Get the local and international domains for the country
        loc = [doc['source_domain'] for doc in db['sources'].find({'primary_location': country_code, 'include': True})]
        ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
        regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]

        # Process local domains
        p_umap(process_domain_counts, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), [True]*len(loc), num_cpus=10)

        # Process international domains
        p_umap(process_domain_counts, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), [False]*len(ints), num_cpus=10)
        p_umap(process_domain_counts, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), [False]*len(regionals), num_cpus=10)

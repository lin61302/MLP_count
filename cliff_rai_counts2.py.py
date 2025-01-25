#!/usr/bin/env python3
import os
from pathlib import Path
import re
import pandas as pd
from tqdm import tqdm
from p_tqdm import p_umap
from dotenv import load_dotenv
from pymongo import MongoClient
import multiprocessing
import dateparser
import time

# Initialize current date
today = pd.Timestamp.now()

# Load environment variables
load_dotenv()
# Hardcoded DB URI for demonstration
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

# Paths to keyword files
__russiapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Russia_RAI_keywords_0730.xlsx'
__chinapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/China_RAI_keywords_0730.xlsx'
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

# Load keyword data
russia = pd.read_excel(__russiapath__)
china = pd.read_excel(__chinapath__)
ru = russia['CompanyName'].str.strip()
ch = china['CompanyName'].str.strip()
ru_ind = russia['alphabet_connect']
ch_ind = china['alphabet_connect']

geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip()
g_loc = geo_loc['CompanyName'].str.strip()

# Compile regex for RAI and Georgia keywords
def compile_regex(keyword_list, boundary_list):
    """
    Compile a single combined regex from keyword_list using the boundaries specified by boundary_list.
    If boundary_list[i] == True, enforce word boundaries; otherwise, just escape the keyword without boundaries.
    """
    patterns = []
    for kw, boundary in zip(keyword_list, boundary_list):
        if boundary:
            # Use lookbehind/lookahead to ensure standalone keywords
            patterns.append(f"(?<![a-zA-Z]){re.escape(kw)}(?![a-zA-Z])")
        else:
            patterns.append(re.escape(kw))
    return re.compile('|'.join(patterns), flags=re.IGNORECASE)

rai_re = compile_regex(pd.concat([ru, ch]), pd.concat([ru_ind, ch_ind]).tolist())
g_int_filter = compile_regex(g_int, [True] * len(g_int))
g_loc_filter = compile_regex(g_loc, [True] * len(g_loc))

title_re = re.compile(r'(china|chinese|russia|russian)', flags=re.IGNORECASE)

def check_georgia(doc, _domain):
    """
    Return True if doc *passes* the Georgia filter (i.e., is included).
    Return False if doc *fails* the Georgia filter.
    """
    try:
        if _domain == 'loc':
            # If the local filter finds a match => doc is about local Georgia => exclude => return False
            return not g_loc_filter.search(doc)
        elif _domain == 'int':
            return not g_int_filter.search(doc)
    except:
        return True  # If any error, default to True (include)

def check_rai(doc):
    try:
        return bool(rai_re.search(doc))
    except:
        return False

def check_title(doc):
    # Specifically checks if "china|chinese|russia|russian" is in the title
    try:
        return bool(title_re.search(doc))
    except:
        return False

def process_document(doc):
    """
    For parallel processing each document to figure out event_type and RAI keywords.
    Returns (doc_id, event_type, keywords_list).
    """
    event_type = '-999'
    keywords = []
    try:
        title_text = doc.get('title_translated', '') or ''
        maintext_text = doc.get('maintext_translated', '') or ''
        combined_text = f"{title_text} {maintext_text[:2000]}"

        if check_rai(combined_text) or check_title(title_text):
            # If it meets RAI logic, set event_type from doc['RAI']['event_type']
            # (Assumes doc['RAI'] has 'event_type' key)
            event_type = doc['RAI'].get('event_type', '-999')
            # Extract keywords matched
            matched = rai_re.findall(combined_text)
            keywords = list(set(matched))  # unique matches
    except:
        pass

    return doc['_id'], event_type, keywords

def update_info(docs, event_types, keywords, colname):
    """
    Bulk update the docs in the DB with event_type_RAI and RAI_keywords.
    """
    db = MongoClient(uri).ml4p
    updates = []
    for idx, doc in enumerate(docs):
        updates.append({
            'filter': {'_id': doc['_id']},
            'update': {'$set': {
                'event_type_RAI': event_types[idx],
                'RAI_keywords': keywords[idx]
            }}
        })
    if updates:
        for up in updates:
            db[colname].update_one(up['filter'], up['update'])


def count_domain_loc(uri, domain, country_name, country_code):
    """
    Exactly replicates V1's logic for local domains, but uses p_umap for parallel keyword checks.
    """
    db = MongoClient(uri).ml4p

    # Build monthly index from 2012-01 to today+31 days
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    # All possible RAI events
    events = list(db.models.find_one({'model_name': 'RAI'}).get('event_type_nums').keys())
    for et in events:
        df[et] = 0
    # Also track -999 to replicate logic
    if '-999' not in events:
        events.append('-999')
        df['-999'] = 0

    for date in df.index:
        colname = f'articles-{date.year}-{date.month}'

        # Non-English docs
        query_non_en = {
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
        }
        docs_non_en = list(db[colname].find(query_non_en, batch_size=1))

        # English docs
        query_en = {
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
        }
        docs_en = list(db[colname].find(query_en, batch_size=1))

        docs = docs_non_en + docs_en

        if len(docs) == 0:
            continue

        # Parallel processing to assign event types & keywords
        results = p_umap(process_document, docs, num_cpus=10)
        doc_ids, event_types, keywords_list = zip(*results)

        # Update DB
        update_info(docs, event_types, keywords_list, colname)

        # Count events for each event type
        for et in events:
            # (if et == '-999', we want those docs that got assigned -999)
            if et == '-999':
                count = sum(1 for idx, doc in enumerate(docs) if event_types[idx] == '-999')
            else:
                count = sum(1 for idx, doc in enumerate(docs)
                            if doc['RAI'].get('event_type') == et and event_types[idx] == et)

            # If country_code == 'GEO', apply Georgia local filter
            if country_code == 'GEO' and count > 0:
                # Recompute count with Georgia local filter
                def passes_geo_filter(i):
                    # doc must have event_types[i] == et to be counted
                    # and also pass check_georgia
                    if et == '-999':
                        assigned = (event_types[i] == '-999')
                    else:
                        assigned = (doc['RAI'].get('event_type') == et and event_types[i] == et)
                    # Then also pass georgia check
                    passed_filter = check_georgia(doc['maintext_translated'][:2000], 'loc') \
                                    and check_georgia(doc['title_translated'], 'loc')
                    return assigned and passed_filter

                new_count = 0
                for i, doc in enumerate(docs):
                    if passes_geo_filter(i):
                        new_count += 1
                count = new_count

            df.loc[date, et] = count

        # Also set 'Country_Georgia' field in DB if needed
        if country_code == 'GEO':
            for i, doc in enumerate(docs):
                main_2000 = doc['maintext_translated'][:2000] if doc.get('maintext_translated') else ''
                title = doc.get('title_translated','')
                if check_georgia(main_2000, 'loc') and check_georgia(title, 'loc'):
                    db[colname].update_one({'_id': doc['_id']},
                                           {'$set': {'Country_Georgia': 'Yes'}})
                else:
                    db[colname].update_one({'_id': doc['_id']},
                                           {'$set': {'Country_Georgia': 'No'}})

    # Export CSV
    outpath = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    Path(outpath).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(outpath, f'{domain}.csv'))

def count_domain_int(uri, domain, country_name, country_code):
    """
    Exactly replicates V1's logic for international/regional domains, also using p_umap for parallel processing.
    """
    db = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    events = list(db.models.find_one({'model_name': 'RAI'}).get('event_type_nums').keys())
    for et in events:
        df[et] = 0
    if '-999' not in events:
        events.append('-999')
        df['-999'] = 0

    for date in df.index:
        colname = f'articles-{date.year}-{date.month}'
        # Non-English: must have 'cliff_locations.country_code' 
        query_non_en = {
            'source_domain': domain,
            'include': True,
            'RAI': {'$exists': True, '$ne': None},
            'language': {'$ne': 'en'},
            f'cliff_locations.{country_code}': {'$exists': True}
        }
        docs_non_en = list(db[colname].find(query_non_en))

        # English: must have 'en_cliff_locations.country_code'
        query_en = {
            'source_domain': domain,
            'include': True,
            'RAI': {'$exists': True, '$ne': None},
            'language': 'en',
            f'en_cliff_locations.{country_code}': {'$exists': True}
        }
        docs_en = list(db[colname].find(query_en))

        docs = docs_non_en + docs_en

        if len(docs) == 0:
            continue

        # Parallel processing
        results = p_umap(process_document, docs, num_cpus=10)
        doc_ids, event_types, keywords_list = zip(*results)

        # Update DB
        update_info(docs, event_types, keywords_list, colname)

        # Count events
        for et in events:
            if et == '-999':
                count = sum(1 for idx, doc in enumerate(docs) if event_types[idx] == '-999')
            else:
                count = sum(1 for idx, doc in enumerate(docs)
                            if doc['RAI'].get('event_type') == et and event_types[idx] == et)

            # If country_code == 'GEO', apply Georgia "int" filter
            if country_code == 'GEO' and count > 0:
                def passes_geo_filter(i):
                    if et == '-999':
                        assigned = (event_types[i] == '-999')
                    else:
                        assigned = (doc['RAI'].get('event_type') == et and event_types[i] == et)
                    passed_filter = check_georgia(doc.get('maintext_translated','')[:2000], 'int') \
                                    and check_georgia(doc.get('title_translated',''), 'int')
                    return assigned and passed_filter

                new_count = 0
                for i, doc in enumerate(docs):
                    if passes_geo_filter(i):
                        new_count += 1
                count = new_count

            df.loc[date, et] = count

        # Update 'Country_Georgia' field if needed
        if country_code == 'GEO':
            for i, doc in enumerate(docs):
                main_2000 = doc.get('maintext_translated','')[:2000]
                title = doc.get('title_translated','')
                if check_georgia(main_2000, 'int') and check_georgia(title, 'int'):
                    db[colname].update_one({'_id': doc['_id']},
                                           {'$set': {'Country_Georgia': 'Yes'}})
                else:
                    db[colname].update_one({'_id': doc['_id']},
                                           {'$set': {'Country_Georgia': 'No'}})

    # Export CSV
    outpath = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    Path(outpath).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(outpath, f'{domain}.csv'))

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

    for country_name, country_code in countries:
        print(f"Starting: {country_name}")

        # Build domain lists
        loc_domains = [doc['source_domain'] for doc in db['sources'].find(
            {'primary_location': country_code, 'include': True})]
        if country_code == 'XKX':
            loc_domains.append('balkaninsight.com')
        elif country_code == 'KAZ':
            loc_domains.append('kaztag.kz')

        int_domains = [doc['source_domain'] for doc in db['sources'].find(
            {'major_international': True, 'include': True})]
        reg_domains = [doc['source_domain'] for doc in db['sources'].find(
            {'major_regional': True, 'include': True})]

        # Process local domains
        while True:
            try:
                p_umap(count_domain_loc,
                       [uri]*len(loc_domains),
                       loc_domains,
                       [country_name]*len(loc_domains),
                       [country_code]*len(loc_domains),
                       num_cpus=10)
                break
            except Exception as e:
                print("Retrying local domains:", e)
                pass

        # Process international domains
        while True:
            try:
                p_umap(count_domain_int,
                       [uri]*len(int_domains),
                       int_domains,
                       [country_name]*len(int_domains),
                       [country_code]*len(int_domains),
                       num_cpus=10)
                break
            except Exception as e:
                print("Retrying international domains:", e)
                pass

        # Process regional domains (treated as international)
        while True:
            try:
                p_umap(count_domain_int,
                       [uri]*len(reg_domains),
                       reg_domains,
                       [country_name]*len(reg_domains),
                       [country_code]*len(reg_domains),
                       num_cpus=10)
                break
            except Exception as e:
                print("Retrying regional domains:", e)
                pass
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
print(today)

__russiapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Russia_RAI_keywords_0730.xlsx'
__chinapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/China_RAI_keywords_0730.xlsx'
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

# Load Russia/China acronyms
russia = pd.read_excel(__russiapath__)
china = pd.read_excel(__chinapath__)
ru = russia['CompanyName'].str.strip()
ch = china['CompanyName'].str.strip()
ru_ind = russia['alphabet_connect']
ch_ind = china['alphabet_connect']

title_re = re.compile(r'(china|chinese|russia|russian)', flags=re.IGNORECASE)

# Compile RAI regex from the acronyms DataFrame
def compile_regex(keyword_list, boundary_list):
    patterns = []
    for kw, boundary in zip(keyword_list, boundary_list):
        kw_stripped = kw.strip()
        if boundary:
            # Word boundary with lookbehind/lookahead
            patterns.append(f"(?<![a-zA-Z]){re.escape(kw_stripped)}(?![a-zA-Z])")
        else:
            patterns.append(re.escape(kw_stripped))
    return re.compile('|'.join(patterns), flags=re.IGNORECASE)

rai_re = compile_regex(pd.concat([ru, ch]), pd.concat([ru_ind, ch_ind]).tolist())

# Load Georgia filters
geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip()
g_loc = geo_loc['CompanyName'].str.strip()

g_int_filter = compile_regex(g_int, [True]*len(g_int))
g_loc_filter = compile_regex(g_loc, [True]*len(g_loc))

events = [k for k in db.models.find_one({'model_name': 'RAI'}).get('event_type_nums').keys()]

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

def update_info(docs, event_types, keywords, colname):
    """
    Updates the docs into the db with event_type_RAI and RAI_keywords.
    """
    db_local = MongoClient(uri).ml4p
    for nn, _doc in enumerate(docs):
        try:
            colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        except:
            dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
            colname = f"articles-{dd.year}-{dd.month}"

        db_local[colname].update_one(
            {'_id': _doc['_id']},
            {'$set': {
                'event_type_RAI': event_types[nn],
                'RAI_keywords': keywords[nn]
            }}
        )

# ------------------------ LOC Domain ------------------------
def count_domain_loc(uri, domain, country_name, country_code):
    db_local = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    for et in events:
        df[et] = 0

    for date in df.index:
        colname = f'articles-{date.year}-{date.month}'

        # Single projection to fetch only needed fields
        projection = {
            '_id': 1, 'RAI': 1, 'date_publish': 1,
            'title_translated': 1, 'maintext_translated': 1
        }

        # Non-English docs
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

        # English docs
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

        if len(docs) == 0:
            continue

        event_types, keywords = [], [[] for _ in range(len(docs))]

        # Step 1: Process documents and filter by RAI keyword checks
        for idx, doc in enumerate(docs):
            maintext_snippet = doc['maintext_translated'][:2000]
            title_text = doc['title_translated']
            if check_rai(maintext_snippet) or check_rai(title_text) or check_title(title_text):
                event_types.append(doc['RAI']['event_type'])
                combined_text = f"{title_text} {maintext_snippet}"
                matches = rai_re.findall(combined_text)
                keywords[idx].extend(matches)
            else:
                event_types.append('-999')

        # Step 2: Remove duplicates from keyword lists
        keywords = [list(set(k)) for k in keywords]

        # Step 3: Update DB
        proc = multiprocessing.Process(target=update_info(docs, event_types, keywords, colname),
                                       args=(docs, event_types, keywords))
        proc.start()
        proc.join()

        # Step 4: Filter + count events
        for et in events:
            if et == '-999':
                count = sum(1 for idx in range(len(docs)) if event_types[idx] == '-999')
            else:
                try:
                    count = sum(1 for idx, d in enumerate(docs)
                                if d['RAI'].get('event_type') == et and event_types[idx] == et)
                except Exception as e:
                    print("Error counting event:", e)
                    count = 0

            # If Georgia, filter again
            if country_code == 'GEO' and count > 0:
                def apply_loc_geo_filter(i):
                    doc = docs[i]
                    return (event_types[i] == et if et != '-999' else event_types[i] == '-999') and \
                           check_georgia(doc['maintext_translated'][:2000], 'loc') and \
                           check_georgia(doc['title_translated'], 'loc')
                count = sum(apply_loc_geo_filter(i) for i in range(len(docs)))

            df.loc[date, et] = count

        # Mark 'Country_Georgia' if needed
        if country_code == 'GEO':
            for doc_i, _doc in enumerate(docs):
                try:
                    col_g = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                except:
                    dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
                    col_g = f"articles-{dd.year}-{dd.month}"
                is_yes = (check_georgia(_doc['maintext_translated'][:2000], 'loc') and
                          check_georgia(_doc['title_translated'], 'loc'))
                db_local[col_g].update_one(
                    {'_id': _doc['_id']},
                    {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}}
                )

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    Path(path).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(path, f'{domain}.csv'))

# ------------------------ INT Domain ------------------------
def count_domain_int(uri, domain, country_name, country_code):
    db_local = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    for et in events:
        df[et] = 0

    for date in df.index:
        colname = f'articles-{date.year}-{date.month}'
        projection = {
            '_id': 1, 'RAI': 1, 'date_publish': 1,
            'title_translated': 1, 'maintext_translated': 1
        }

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

        if len(docs) == 0:
            continue

        event_types, keywords = [], [[] for _ in range(len(docs))]

        # Step 1: doc-level checks
        for idx, doc in enumerate(docs):
            maintext_snippet = doc['maintext_translated'][:2000]
            title_text = doc['title_translated']
            if check_rai(maintext_snippet) or check_rai(title_text) or check_title(title_text):
                event_types.append(doc['RAI']['event_type'])
                combined_text = f"{title_text} {maintext_snippet}"
                matches = rai_re.findall(combined_text)
                keywords[idx].extend(matches)
            else:
                event_types.append('-999')

        # Step 2: deduplicate keywords
        keywords = [list(set(k)) for k in keywords]

        # Step 3: update DB
        proc = multiprocessing.Process(target=update_info(docs, event_types, keywords, colname),
                                       args=(docs, event_types, keywords))
        proc.start()
        proc.join()

        # Step 4: filter + count
        for et in events:
            if et == '-999':
                count = sum(1 for idx in range(len(docs)) if event_types[idx] == '-999')
            else:
                count = sum(1 for idx, d in enumerate(docs)
                            if d['RAI'].get('event_type') == et and event_types[idx] == et)

            if country_code == 'GEO' and count > 0:
                def apply_int_geo_filter(i):
                    doc = docs[i]
                    return (event_types[i] == et if et != '-999' else event_types[i] == '-999') and \
                           check_georgia(doc['maintext_translated'][:2000], 'int') and \
                           check_georgia(doc['title_translated'], 'int')
                count = sum(apply_int_geo_filter(i) for i in range(len(docs)))

            df.loc[date, et] = count

        if country_code == 'GEO':
            for _doc in docs:
                try:
                    col_g = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                except:
                    dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
                    col_g = f"articles-{dd.year}-{dd.month}"

                is_yes = (check_georgia(_doc['maintext_translated'], 'int') and
                          check_georgia(_doc['title_translated'], 'int'))
                db_local[col_g].update_one(
                    {'_id': _doc['_id']},
                    {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}}
                )

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
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
    

    countries_needed = ['GEO','TLS','MOZ','MLI','KAZ','ARM']

    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

    for ctup in countries:
        print('Starting:', ctup[0])
        country_name = ctup[0]
        country_code = ctup[1]

        # Build local, int, regional domain lists as in V1
        if country_code == 'XKX':
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {'primary_location': {'$in':[country_code]}, 'include': True}
            )]+['balkaninsight.com']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international':True, 'include':True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional':True, 'include':True}) if doc['source_domain']!='balkaninsight.com']
        elif country_code == 'KAZ':
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {'primary_location': {'$in':[country_code]}, 'include': True}
            )]+['kaztag.kz']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international':True, 'include':True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional':True, 'include':True})]
        else:
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {'primary_location':{'$in':[country_code]}, 'include': True}
            )]
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international':True, 'include':True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional':True, 'include':True})]

        ind = 1
        while ind:
            try:
                p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)
                ind = 0
            except Exception as err:
                print("Retrying local domains:", err)
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
        commit_message = f"RAI count ({countries_added}) update"
        # run_git_commands(commit_message)

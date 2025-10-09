#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merged civic counters: runs **civic_new** and **civic_related** in a single pass per domain
without changing output formats. It reuses the original classification logic
to set `event_type_civic_new` and simultaneously produces:
- Combined counts (as civic_new did)
- Civic_Related and Non_Civic_Related counts (as civic_related did),
while avoiding duplicate DB scans per month/domain.
"""
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

load_dotenv()
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p
today = pd.Timestamp.now() #- pd.Timedelta(days=1)

# === Event list, regex rules, and Georgia filters (from civic_new.py) ===

events = [k for k in db.models.find_one({'model_name': 'civic_new'}).get('event_type_nums').keys()] + ['defamationcase','total_articles']

censor_re = re.compile(r'\b(freedom\w*|assembl\w*|associat\w*|term limit\w*|independen\w*|succession\w*|demonstrat\w*|crackdown\w*|draconian|censor\w*|authoritarian|repress\w*|NGO\b|human rights|journal\w*|newspaper|media|outlet|reporter|broadcast\w*|correspondent|press|magazine|paper|black out|blacklist|suppress|speaking|false news|fake news|radio|commentator|blogger|opposition voice|voice of the opposition|speech|publish)\b', flags=re.IGNORECASE)
defame_re1 = re.compile(r'\b(case|lawsuit|sue|suing|suit|trial|court|charge\w*|rule|ruling|sentence|sentencing|judg\w*)\b', flags=re.IGNORECASE)
defame_re2 = re.compile(r'\b(defamation|defame|libel|slander|insult|reputation|lese majeste|lese majesty|lese-majeste)\b', flags=re.IGNORECASE)

double_re = re.compile(r'\b(embezzle\w*|bribe\w*|gift\w*|fraud\w*|corrupt\w*|procure\w*|budget|assets|irregularities|graft|enrich\w*|laundering)\b', flags=re.IGNORECASE)
corrupt_LA_re = re.compile(r'\b(legal process|case|investigat\w*|appeal|prosecut\w*|lawsuit|sue|suing|trial|court|charg\w*|rule|ruling|sentenc\w*|judg\w*)\b', flags=re.IGNORECASE)
corrupt_AR_re = re.compile(r'\b(arrest|detain|apprehend|captur\w*|custod\w*|imprison|jail)\b', flags=re.IGNORECASE)
corrupt_PU_re = re.compile(r'\b(resign|fire|firing|dismiss|sack|replac\w*|quit)\b', flags=re.IGNORECASE)

coup_re = re.compile(r'((?<![a-zA-Z])coup(?<![a-zA-Z])|(?<![a-zA-Z])coups(?<![a-zA-Z])|(?<![a-zA-Z])depose|(?<![a-zA-Z])overthrow|(?<![a-zA-Z])oust)', flags=re.IGNORECASE)
ukr_re = re.compile(r'(ukrain.*)', flags=re.IGNORECASE)

__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

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

def check_censorship(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(censor_re.search(title_t)) or bool(censor_re.search(main_t))
    except:
        return False

def check_defamation(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        cond1 = bool(defame_re2.search(title_t)) or bool(defame_re2.search(main_t))
        cond2 = bool(defame_re1.search(title_t)) or bool(defame_re1.search(main_t))
        return cond1 and cond2
    except:
        return False

def check_double(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(double_re.search(title_t)) or bool(double_re.search(main_t))
    except:
        return False

def check_corruption_LA(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(corrupt_LA_re.search(title_t)) or bool(corrupt_LA_re.search(main_t))
    except:
        return False

def check_corruption_AR(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(corrupt_AR_re.search(title_t)) or bool(corrupt_AR_re.search(main_t))
    except:
        return False

def check_corruption_PU(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(corrupt_PU_re.search(title_t)) or bool(corrupt_PU_re.search(main_t))
    except:
        return False

def check_coup(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(coup_re.search(title_t)) or bool(coup_re.search(main_t))
    except:
        return False

def check_ukr(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(ukr_re.search(title_t)) or bool(ukr_re.search(main_t))
    except:
        return False



# === Helper functions (from civic_new.py) ===

def check_georgia(doc, _domain):
    try:
        if _domain == 'loc':
            return not g_loc_filter.search(doc)
        else:  # 'int'
            return not g_int_filter.search(doc)
    except:
        return True



def check_censorship(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(censor_re.search(title_t)) or bool(censor_re.search(main_t))
    except:
        return False



def check_defamation(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        cond1 = bool(defame_re2.search(title_t)) or bool(defame_re2.search(main_t))
        cond2 = bool(defame_re1.search(title_t)) or bool(defame_re1.search(main_t))
        return cond1 and cond2
    except:
        return False



def check_double(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(double_re.search(title_t)) or bool(double_re.search(main_t))
    except:
        return False



def check_corruption_LA(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(corrupt_LA_re.search(title_t)) or bool(corrupt_LA_re.search(main_t))
    except:
        return False



def check_corruption_AR(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(corrupt_AR_re.search(title_t)) or bool(corrupt_AR_re.search(main_t))
    except:
        return False



def check_corruption_PU(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(corrupt_PU_re.search(title_t)) or bool(corrupt_PU_re.search(main_t))
    except:
        return False



def check_coup(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(coup_re.search(title_t)) or bool(coup_re.search(main_t))
    except:
        return False



def check_ukr(doc):
    try:
        title_t = doc.get('title_translated','')
        main_t = doc.get('maintext_translated','')
        return bool(ukr_re.search(title_t)) or bool(ukr_re.search(main_t))
    except:
        return False



def update_info(docs, event_types, event_types2, colname):
    db_local = MongoClient(uri).ml4p
    for nn, _doc in enumerate(docs):
        try:
            colname_new = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        except:
            dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
            colname_new = f"articles-{dd.year}-{dd.month}"
        db_local[colname_new].update_one(
            {'_id': _doc['_id']},
            {'$set': {
                'event_type_civic_new': event_types[nn],
                'event_type_civic_new_2': event_types2[nn]
            }}
        )



def add_ukr(docs_ukr):
    db_local = MongoClient(uri).ml4p
    for _doc in docs_ukr:
        try:
            colname_new = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        except:
            dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
            colname_new = f"articles-{dd.year}-{dd.month}"

        existing_doc = db_local[colname_new].find_one({'_id': _doc['_id']})
        if existing_doc:
            cliff_locations = existing_doc.get('cliff_locations', {})
            if 'UKR' in cliff_locations:
                if 'Ukraine' not in cliff_locations['UKR']:
                    cliff_locations['UKR'].insert(0, 'Ukraine')
            else:
                cliff_locations['UKR'] = ['Ukraine']
            db_local[colname_new].update_one(
                {'_id': _doc['_id']},
                {'$set': {'cliff_locations': cliff_locations}}
            )
        else:
            db_local[colname_new].update_one(
                {'_id': _doc['_id']},
                {'$set': {'cliff_locations.UKR': ['Ukraine']}}
            )



# === Merged counting functions ===


def _prepare_df(today, events):
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month
    for et in events:
        df[et] = 0
    return df

def _project_common():
    return {
        '_id': 1, 'civic_new': 1, 'civic_related': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'cliff_locations': 1, 'en_cliff_locations': 1, 'language': 1, 'source_domain': 1
    }

def _compute_event_types(docs):
    # Start with raw model labels
    event_types = [ (d.get('civic_new') or {}).get('event_type') for d in docs ]
    event_types2 = [ None for _ in docs ]

    for idx, d in enumerate(docs):
        e_type = event_types[idx]
        if not e_type:
            continue
        # Legal action special cases: coup, defamation, or corruption (double)
        if e_type == 'legalaction':
            if check_coup(d):
                event_types[idx] = 'legalaction'
                event_types2[idx] = 'coup'
            elif check_defamation(d):
                event_types[idx] = 'legalaction'
                event_types2[idx] = 'defamationcase'
            else:
                event_types[idx] = 'legalaction'
                if check_double(d):
                    event_types2[idx] = 'corruption'

        elif e_type == 'censor':
            if check_censorship(d):
                event_types[idx] = 'censor'
            else:
                # mark as invalid so it won't be counted
                event_types[idx] = '-999'

        elif e_type == 'arrest':
            if check_double(d):
                event_types2[idx] = 'corruption'

        elif e_type == 'purge':
            if check_double(d):
                event_types2[idx] = 'corruption'

        elif e_type == 'corruption':
            # keep as corruption; nuanced redistribution happens in counting via event_types2
            event_types[idx] = 'corruption'
        else:
            event_types[idx] = e_type

    return event_types, event_types2

def _apply_georgia_filter(docs, country_code, domain_kind):
    if country_code != 'GEO':
        return docs
    # Only keep docs that pass the Georgia false-positive filter
    out = []
    for d in docs:
        title_t = d.get('title_translated','')
        main_t = d.get('maintext_translated','')
        ok = check_georgia(title_t, 'loc' if domain_kind=='loc' else 'int') and \
             check_georgia(main_t, 'loc' if domain_kind=='loc' else 'int')
        if ok:
            out.append(d)
    return out

def _count_events_from_types(event_types, event_types2, events):
    # Turn (event_types, event_types2) into counts for each event in 'events'.
    from collections import Counter
    n = len(event_types)
    idxs = range(n)

    # Precompute indices per label
    idx_by_e1 = {}
    for i in idxs:
        idx_by_e1.setdefault(event_types[i], []).append(i)
    idx_by_e2 = {}
    for i in idxs:
        if event_types2[i]:
            idx_by_e2.setdefault(event_types2[i], []).append(i)

    counts = {et: 0 for et in events}
    total = n
    for et in events:
        if et == 'total_articles':
            counts[et] = total
        elif et == 'defamationcase':
            counts[et] = len(idx_by_e2.get('defamationcase', []))
        elif et == 'corruption':
            # corruption if primary is corruption OR secondary flagged as corruption
            counts[et] = len(idx_by_e1.get('corruption', [])) + len(idx_by_e2.get('corruption', []))
        else:
            counts[et] = len(idx_by_e1.get(et, []))
    return counts

def _split_related(docs):
    yes_idx, no_idx = [], []
    for i, d in enumerate(docs):
        cr = (d.get('civic_related') or {}).get('result')
        if cr == 'Yes':
            yes_idx.append(i)
        elif cr == 'No':
            no_idx.append(i)
        else:
            # ignore if related score missing (to match original '$exists: True' behavior)
            pass
    return yes_idx, no_idx

def _subset(lst, indices):
    return [lst[i] for i in indices]

def count_domain_loc_merged(uri, domain, country_name, country_code):
    db = MongoClient(uri).ml4p
    df_combined = _prepare_df(today, events)
    df_rel = _prepare_df(today, events)
    df_nonrel = _prepare_df(today, events)

    projection = _project_common()

    for date in df_combined.index:
        colname = f"articles-{date.year}-{date.month}"

        # Local: Non-English using 'cliff_locations', English using 'en_cliff_locations'
        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
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

        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
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

        # Compute event labels for these docs
        e1, e2 = _compute_event_types(docs)

        # Update DB with event_type_civic_new fields (async, as in original)
        try:
            proc2 = multiprocessing.Process(target=update_info, args=(docs, e1, e2, colname))
            proc2.start()
        except Exception as err:
            print("Failed to spawn update_info:", err)

        # Ukraine side-effect (keep original behavior)
        docs_ukr = [d for d in docs if check_ukr(d)]
        try:
            proc3 = multiprocessing.Process(target=add_ukr, args=(docs_ukr,))
            proc3.start()
        except Exception as err:
            print("Failed to spawn add_ukr:", err)

        # Georgia filter for local
        docs = _apply_georgia_filter(docs, country_code, 'loc')
        # Also filter e1/e2 arrays to match filtered docs
        if len(docs) != len(e1):
            # rebuild e1/e2 for filtered docs
            e1, e2 = _compute_event_types(docs)

        # Fill combined counts
        counts_all = _count_events_from_types(e1, e2, events)
        for et, v in counts_all.items():
            df_combined.loc[date, et] = v

        # Related split
        yes_idx, no_idx = _split_related(docs)
        e1_yes, e2_yes = _subset(e1, yes_idx), _subset(e2, yes_idx)
        e1_no,  e2_no  = _subset(e1, no_idx),  _subset(e2, no_idx)
        counts_yes = _count_events_from_types(e1_yes, e2_yes, events)
        counts_no  = _count_events_from_types(e1_no,  e2_no,  events)

        for et, v in counts_yes.items():
            df_rel.loc[date, et] = v
        for et, v in counts_no.items():
            df_nonrel.loc[date, et] = v

        # Country_Georgia tag (side-effect) to match original
        if country_code == 'GEO':
            for _doc in docs:
                try:
                    try:
                        colname_g = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                    except Exception:
                        dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
                        colname_g = f"articles-{dd.year}-{dd.month}"
                    is_yes = check_georgia(_doc.get('maintext_translated',''), 'loc') and \
                             check_georgia(_doc.get('title_translated',''), 'loc')
                    db[colname_g].update_one({'_id': _doc['_id']}, {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}})
                except Exception as _:
                    pass

    # Write outputs
    combined_path = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Combined/"
    Path(combined_path).mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(os.path.join(combined_path, f"{domain}.csv"))

    civic_rel_path = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Civic_Related/"
    non_rel_path   = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Non_Civic_Related/"
    Path(civic_rel_path).mkdir(parents=True, exist_ok=True)
    Path(non_rel_path).mkdir(parents=True, exist_ok=True)
    df_rel.to_csv(os.path.join(civic_rel_path, f"{domain}.csv"))
    df_nonrel.to_csv(os.path.join(non_rel_path, f"{domain}.csv"))

def count_domain_int_merged(uri, domain, country_name, country_code):
    db = MongoClient(uri).ml4p
    df_combined = _prepare_df(today, events)
    df_rel = _prepare_df(today, events)
    df_nonrel = _prepare_df(today, events)

    projection = _project_common()

    for date in df_combined.index:
        colname = f"articles-{date.year}-{date.month}"

        # International: must explicitly mention the country in the location fields
        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': {'$ne': 'en'},
                f'cliff_locations.{country_code}': {'$exists': True}
            },
            projection=projection,
            batch_size=100
        )
        docs1 = list(cur1)

        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': 'en',
                f'en_cliff_locations.{country_code}': {'$exists': True}
            },
            projection=projection,
            batch_size=100
        )
        docs2 = list(cur2)

        docs = docs1 + docs2

        # Compute event labels for these docs
        e1, e2 = _compute_event_types(docs)

        # Update DB with event_type_civic_new fields (async)
        try:
            proc2 = multiprocessing.Process(target=update_info, args=(docs, e1, e2, colname))
            proc2.start()
        except Exception as err:
            print("Failed to spawn update_info:", err)

        # Georgia filter for international
        docs = _apply_georgia_filter(docs, country_code, 'int')
        if len(docs) != len(e1):
            e1, e2 = _compute_event_types(docs)

        # Fill combined counts
        counts_all = _count_events_from_types(e1, e2, events)
        for et, v in counts_all.items():
            df_combined.loc[date, et] = v

        # Related split
        yes_idx, no_idx = _split_related(docs)
        e1_yes, e2_yes = _subset(e1, yes_idx), _subset(e2, yes_idx)
        e1_no,  e2_no  = _subset(e1, no_idx),  _subset(e2, no_idx)
        counts_yes = _count_events_from_types(e1_yes, e2_yes, events)
        counts_no  = _count_events_from_types(e1_no,  e2_no,  events)

        for et, v in counts_yes.items():
            df_rel.loc[date, et] = v
        for et, v in counts_no.items():
            df_nonrel.loc[date, et] = v

    # Write outputs
    combined_path = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Combined/"
    Path(combined_path).mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(os.path.join(combined_path, f"{domain}.csv"))

    civic_rel_path = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Civic_Related/"
    non_rel_path   = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Non_Civic_Related/"
    Path(civic_rel_path).mkdir(parents=True, exist_ok=True)
    Path(non_rel_path).mkdir(parents=True, exist_ok=True)
    df_rel.to_csv(os.path.join(civic_rel_path, f"{domain}.csv"))
    df_nonrel.to_csv(os.path.join(non_rel_path, f"{domain}.csv"))


# === Git helper (unchanged) ===

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

# === Country lists (from civic_new.py) ===




# === Main (from civic_new.py, calling merged counters) ===
if __name__ == '__main__':
    slp = False
    if slp:
        t = 7200
        print(f'start sleeping for {t/60} mins')
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
        ('Panama','PAN'),
        ('Mexico','MEX')
        
    ]
    

    countries_needed = [
        
                            # 'COL', 'ECU',  'PRY','JAM','HND', 'SLV', 'NIC','PER', 'DOM','PAN', 'CRI','SLB', 
                            # 'BGD','NGA','UGA',
                            #    'ALB', 'BEN', 'ETH', 'GEO', 'KEN', 'MLI', 'MAR',   
                            #    'SRB', 'SEN', 'TZA', 'UKR', 'ZWE', 'MRT', 'ZMB', 'XKX', 'NER',  
                            #     'PHL', 'GHA', 'RWA', 'GTM', 'BLR', 'KHM', 'COD', 'TUR', 
                            #    'ZAF', 'TUN', 'IDN', 'AGO', 'ARM', 'LKA', 'MYS', 'CMR', 'HUN', 'MWI', 
                            #    'UZB', 'IND', 'MOZ', 'AZE', 'KGZ', 'MDA', 'KAZ', 'DZA', 'MKD', 'SSD', 
                            #    'LBR', 'PAK', 'NPL', 'NAM', 'BFA', 'TLS', #'MEX'
                            # 'MEX','UZB',
                              'MOZ','COD','SSD','ZWE','GHA','KHM'
                            # 'IND'
                               ]
    # countries_needed = ['PHL','BFA','AGO','AZE','MWI','BLR','BGD','HUN','XKX','MYS']

    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

    for ctup in countries:
        print('Starting:', ctup[0])
        country_name = ctup[0]
        country_code = ctup[1]

        if country_code == 'XKX':
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {'primary_location': {'$in':[country_code]}, 'include': True}
            )]+['balkaninsight.com']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True}) if doc['source_domain']!='balkaninsight.com']
        elif country_code == 'KAZ':
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {'primary_location': {'$in':[country_code]}, 'include': True}
            )]+['kaztag.kz']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]
        else:
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {'primary_location': {'$in':[country_code]}, 'include': True}
            )]
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]

        ind = 1
        while ind:
            try:
                p_umap(count_domain_loc_merged, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)
                ind = 0
            except Exception as err:
                print(err)
                pass

        ind = 1
        while ind:
            try:
                p_umap(count_domain_int_merged, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=10)
                ind = 0
            except Exception as err:
                print(err)
                pass

        ind = 1
        while ind:
            try:
                p_umap(count_domain_int_merged, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=10)
                ind = 0
            except:
                pass
        
        # Git operations
        countries_added = '/'.join(countries_needed)
        commit_message = f"civic count ({countries_added}) update"
        run_git_commands(commit_message)

# screen -S screen_count
# screen -r screen_count
# conda activate peace

# cd /home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts
#'ETH','TZA','BEN','COL','ECU','DZA','NIC','KEN','JAM','GTM','MLI','SEN','ZWE','COD'

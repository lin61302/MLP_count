#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Civic counters with source-level and country-level normalization.

Normalization denominator per month = ALL articles considered local
across ALL sources (locals lenient, int/reg strict), with no civic filters.

Outputs:
- New country-level (norm):
  Counts_Civic_New/Final_Aggregated/{country}/{YYYY_M_D}/{country}_Combined.csv
  Counts_Civic_New/Final_Aggregated/{country}/{YYYY_M_D}/{country}_Civic_Related.csv
  Counts_Civic_New/Final_Aggregated/{country}/{YYYY_M_D}/{country}_Non_Civic_Related.csv
- New by-source normalized (divide every numeric col except year/month by country-month denominator):
  Counts_Civic_New/Normalized_By_Source/{country}/{YYYY_M_D}/Combined/{domain}.csv
  Counts_Civic_New/Normalized_By_Source/{country}/{YYYY_M_D}/Civic_Related/{domain}.csv
  Counts_Civic_New/Normalized_By_Source/{country}/{YYYY_M_D}/Non_Civic_Related/{domain}.csv
- New by-source raw:
  Counts_Civic_New/Raw_By_Source/{country}/{YYYY_M_D}/Combined/{domain}.csv
  Counts_Civic_New/Raw_By_Source/{country}/{YYYY_M_D}/Civic_Related/{domain}.csv
  Counts_Civic_New/Raw_By_Source/{country}/{YYYY_M_D}/Non_Civic_Related/{domain}.csv
"""
import os
from pathlib import Path
import re
import numpy as np
import pandas as pd
from tqdm import tqdm
from p_tqdm import p_umap
import time
from dotenv import load_dotenv
from pymongo import MongoClient
import multiprocessing
import dateparser
import subprocess

# -----------------------------
# Setup
# -----------------------------
load_dotenv()
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p
today = pd.Timestamp.now()   # use EOM index below (freq='M')

# === Event list and regex rules (as in your script) ===
events = [k for k in db.models.find_one({'model_name': 'civic_new'}).get('event_type_nums').keys()] + ['defamationcase','total_articles']
civic_category_labels = [e for e in events if e != 'total_articles']  # for _norm columns at country-level

censor_re = re.compile(r'\b(freedom\w*|assembl\w*|associat\w*|term limit\w*|independen\w*|succession\w*|demonstrat\w*|crackdown\w*|draconian|censor\w*|authoritarian|repress\w*|NGO\b|human rights|journal\w*|newspaper|media|outlet|reporter|broadcast\w*|correspondent|press|magazine|paper|black out|blacklist|suppress|speaking|false news|fake news|radio|commentator|blogger|opposition voice|voice of the opposition|speech|publish)\b', flags=re.IGNORECASE)
defame_re1 = re.compile(r'\b(case|lawsuit|sue|suing|suit|trial|court|charge\w*|rule|ruling|sentence|sentencing|judg\w*)\b', flags=re.IGNORECASE)
defame_re2 = re.compile(r'\b(defamation|defame|libel|slander|insult|reputation|lese majeste|lese majesty|lese-majeste)\b', flags=re.IGNORECASE)

double_re = re.compile(r'\b(embezzle\w*|bribe\w*|gift\w*|fraud\w*|corrupt\w*|procure\w*|budget|assets|irregularities|graft|enrich\w*|laundering)\b', flags=re.IGNORECASE)
corrupt_LA_re = re.compile(r'\b(legal process|case|investigat\w*|appeal|prosecut\w*|lawsuit|sue|suing|trial|court|charg\w*|rule|ruling|sentenc\w*|judg\w*)\b', flags=re.IGNORECASE)
corrupt_AR_re = re.compile(r'\b(arrest|detain|apprehend|captur\w*|custod\w*|imprison|jail)\b', flags=re.IGNORECASE)
corrupt_PU_re = re.compile(r'\b(resign|fire|firing|dismiss|sack|replac\w*|quit)\b', flags=re.IGNORECASE)

coup_re = re.compile(r'((?<![a-zA-Z])coup(?<![a-zA-Z])|(?<![a-zA-Z])coups(?<![a-zA-Z])|(?<![a-zA-Z])depose|(?<![a-zA-Z])overthrow|(?<![a-zA-Z])oust)', flags=re.IGNORECASE)
ukr_re = re.compile(r'(ukrain.*)', flags=re.IGNORECASE)

# === Georgia false-positive filters ===
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip().tolist()
g_loc = geo_loc['CompanyName'].str.strip().tolist()

for i, s in enumerate(g_int):
    g_int[i] = "(?<![a-zA-Z])" + s[2:-2].strip() + "(?<![a-zA-Z])"
for i, s in enumerate(g_loc):
    g_loc[i] = "(?<![a-zA-Z])" + s[2:-2].strip() + "(?<![a-zA-Z])"

g_int_string = '|'.join(g_int) if g_int else r'^\b$'
g_loc_string = '|'.join(g_loc) if g_loc else r'^\b$'
g_int_filter = re.compile(g_int_string, flags=re.IGNORECASE)
g_loc_filter = re.compile(g_loc_string, flags=re.IGNORECASE)

def check_georgia_text(text, mode):
    try:
        if mode == 'loc':
            return not g_loc_filter.search(text or "")
        else:
            return not g_int_filter.search(text or "")
    except Exception:
        return True

# === Civic helper checks ===
def check_censorship(doc):
    try:
        return bool(censor_re.search(doc.get('title_translated',''))) or bool(censor_re.search(doc.get('maintext_translated','')))
    except:
        return False

def check_defamation(doc):
    try:
        t, m = doc.get('title_translated',''), doc.get('maintext_translated','')
        return (bool(defame_re2.search(t)) or bool(defame_re2.search(m))) and (bool(defame_re1.search(t)) or bool(defame_re1.search(m)))
    except:
        return False

def check_double(doc):
    try:
        return bool(double_re.search(doc.get('title_translated',''))) or bool(double_re.search(doc.get('maintext_translated','')))
    except:
        return False

def check_corruption_LA(doc):
    try:
        return bool(corrupt_LA_re.search(doc.get('title_translated',''))) or bool(corrupt_LA_re.search(doc.get('maintext_translated','')))
    except:
        return False

def check_corruption_AR(doc):
    try:
        return bool(corrupt_AR_re.search(doc.get('title_translated',''))) or bool(corrupt_AR_re.search(doc.get('maintext_translated','')))
    except:
        return False

def check_corruption_PU(doc):
    try:
        return bool(corrupt_PU_re.search(doc.get('title_translated',''))) or bool(corrupt_PU_re.search(doc.get('maintext_translated','')))
    except:
        return False

def check_coup(doc):
    try:
        return bool(coup_re.search(doc.get('title_translated',''))) or bool(coup_re.search(doc.get('maintext_translated','')))
    except:
        return False

def check_ukr(doc):
    try:
        return bool(ukr_re.search(doc.get('title_translated',''))) or bool(ukr_re.search(doc.get('maintext_translated','')))
    except:
        return False

# === Frames ===
def _prepare_df_eom():
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-01-01', today + pd.Timedelta(31,'d'), freq='M')  # EOM
    df = df.set_index('date')
    df['year'] = df.index.year
    df['month'] = df.index.month
    for et in events:
        df[et] = 0
    return df

def _prepare_denom_eom():
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-01-01', today + pd.Timedelta(31,'d'), freq='M')  # EOM
    df = df.set_index('date')
    df['year'] = df.index.year
    df['month'] = df.index.month
    df['denom_total_local'] = 0
    return df

# === Common projection for find ===
def _project_common():
    return {
        '_id': 1, 'civic_new': 1, 'civic_related': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'cliff_locations': 1, 'en_cliff_locations': 1, 'language': 1, 'source_domain': 1
    }

# === Civic type computation and related split ===
def _compute_event_types(docs):
    et1 = [(d.get('civic_new') or {}).get('event_type') for d in docs]
    et2 = [None for _ in docs]
    for i, d in enumerate(docs):
        e = et1[i]
        if not e:
            continue
        if e == 'legalaction':
            if check_coup(d):
                et1[i] = 'legalaction'; et2[i] = 'coup'
            elif check_defamation(d):
                et1[i] = 'legalaction'; et2[i] = 'defamationcase'
            else:
                et1[i] = 'legalaction'
                if check_double(d):
                    et2[i] = 'corruption'
        elif e == 'censor':
            et1[i] = 'censor' if check_censorship(d) else '-999'
        elif e == 'arrest':
            if check_double(d): et2[i] = 'corruption'
        elif e == 'purge':
            if check_double(d): et2[i] = 'corruption'
        elif e == 'corruption':
            et1[i] = 'corruption'
        else:
            et1[i] = e
    return et1, et2

def _count_events_from_types(et1, et2):
    from collections import Counter
    n = len(et1)
    idx_by_e1, idx_by_e2 = {}, {}
    for i in range(n):
        idx_by_e1.setdefault(et1[i], []).append(i)
        if et2[i]:
            idx_by_e2.setdefault(et2[i], []).append(i)
    counts = {et: 0 for et in events}
    counts['total_articles'] = n
    for et in events:
        if et in ('defamationcase','total_articles'):
            continue
        counts[et] = len(idx_by_e1.get(et, []))
    counts['defamationcase'] = len(idx_by_e2.get('defamationcase', []))
    # 'corruption' already included as primary; secondary corruption is *additional* signal?
    counts['corruption'] += len(idx_by_e2.get('corruption', []))
    return counts

def _split_related(docs):
    yes_idx, no_idx = [], []
    for i, d in enumerate(docs):
        cr = (d.get('civic_related') or {}).get('result')
        if cr == 'Yes': yes_idx.append(i)
        elif cr == 'No': no_idx.append(i)
    return yes_idx, no_idx

def _subset(lst, indices):
    return [lst[i] for i in indices]

# === Side effects ===
def update_info(docs, event_types, event_types2, colname):
    db_local = MongoClient(uri).ml4p
    for nn, _doc in enumerate(docs):
        try:
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
        except:
            pass

def add_ukr(docs_ukr):
    db_local = MongoClient(uri).ml4p
    for _doc in docs_ukr:
        try:
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
        except:
            pass

# === Per-domain merged civic counting (numerators) ===
def _apply_georgia_filter_docs(docs, country_code, mode):
    if country_code != 'GEO':
        return docs
    out = []
    for d in docs:
        t, m = d.get('title_translated',''), d.get('maintext_translated','')
        if check_georgia_text(t, mode) and check_georgia_text(m, mode):
            out.append(d)
    return out

def count_domain_loc_merged(uri, domain, country_name, country_code):
    dbm = MongoClient(uri).ml4p
    df_combined = _prepare_df_eom()
    df_rel = _prepare_df_eom()
    df_nonrel = _prepare_df_eom()
    projection = _project_common()

    for date in df_combined.index:
        colname = f"articles-{date.year}-{date.month}"
        # locals: lenient location rule
        q_non_en = {'source_domain': domain, 'include': True, 'civic_new': {'$exists': True}, 'language': {'$ne': 'en'},
                    '$or': [{f'cliff_locations.{country_code}': {'$exists': True}},
                            {'cliff_locations': {}}]}
        q_en     = {'source_domain': domain, 'include': True, 'civic_new': {'$exists': True}, 'language': 'en',
                    '$or': [{f'en_cliff_locations.{country_code}': {'$exists': True}},
                            {'en_cliff_locations': {}}]}

        docs = list(dbm[colname].find(q_non_en, projection=projection, batch_size=100)) + \
               list(dbm[colname].find(q_en,     projection=projection, batch_size=100))

        # compute event types
        e1, e2 = _compute_event_types(docs)

        # async side-effects (same as original)
        try:
            multiprocessing.Process(target=update_info, args=(docs, e1, e2, colname)).start()
        except Exception as err:
            print("Failed to spawn update_info:", err)

        docs_ukr = [d for d in docs if check_ukr(d)]
        try:
            multiprocessing.Process(target=add_ukr, args=(docs_ukr,)).start()
        except Exception as err:
            print("Failed to spawn add_ukr:", err)

        # Georgia local filter
        docs = _apply_georgia_filter_docs(docs, country_code, 'loc')
        if len(docs) != len(e1):
            e1, e2 = _compute_event_types(docs)

        # combined counts
        counts_all = _count_events_from_types(e1, e2)
        for et, v in counts_all.items():
            df_combined.loc[date, et] = v

        # civic_related split
        yes_idx, no_idx = _split_related(docs)
        e1_yes, e2_yes = _subset(e1, yes_idx), _subset(e2, yes_idx)
        e1_no,  e2_no  = _subset(e1, no_idx),  _subset(e2, no_idx)

        counts_yes = _count_events_from_types(e1_yes, e2_yes)
        counts_no  = _count_events_from_types(e1_no,  e2_no)

        for et, v in counts_yes.items():
            df_rel.loc[date, et] = v
        for et, v in counts_no.items():
            df_nonrel.loc[date, et] = v

        # Country_Georgia tag similar to prior behavior
        if country_code == 'GEO':
            for _doc in docs:
                try:
                    try:
                        colname_g = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                    except Exception:
                        dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
                        colname_g = f"articles-{dd.year}-{dd.month}"
                    is_yes = check_georgia_text(_doc.get('maintext_translated',''), 'loc') and \
                             check_georgia_text(_doc.get('title_translated',''), 'loc')
                    dbm[colname_g].update_one({'_id': _doc['_id']}, {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}})
                except:
                    pass

    # write original per-source raw outputs (unchanged)
    base = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/Raw_By_Source/{country_name}/{today.year}_{today.month}_{today.day}/"
    Path(base + "Combined/").mkdir(parents=True, exist_ok=True)
    Path(base + "Civic_Related/").mkdir(parents=True, exist_ok=True)
    Path(base + "Non_Civic_Related/").mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(os.path.join(base, "Combined", f"{domain}.csv"))
    df_rel.to_csv(os.path.join(base, "Civic_Related", f"{domain}.csv"))
    df_nonrel.to_csv(os.path.join(base, "Non_Civic_Related", f"{domain}.csv"))

    return df_combined, df_rel, df_nonrel

def count_domain_int_merged(uri, domain, country_name, country_code):
    dbm = MongoClient(uri).ml4p
    df_combined = _prepare_df_eom()
    df_rel = _prepare_df_eom()
    df_nonrel = _prepare_df_eom()
    projection = _project_common()

    for date in df_combined.index:
        colname = f"articles-{date.year}-{date.month}"
        # int/reg: strict location rule
        q_non_en = {'source_domain': domain, 'include': True, 'civic_new': {'$exists': True}, 'language': {'$ne': 'en'},
                    f'cliff_locations.{country_code}': {'$exists': True}}
        q_en     = {'source_domain': domain, 'include': True, 'civic_new': {'$exists': True}, 'language': 'en',
                    f'en_cliff_locations.{country_code}': {'$exists': True}}

        docs = list(dbm[colname].find(q_non_en, projection=projection, batch_size=100)) + \
               list(dbm[colname].find(q_en,     projection=projection, batch_size=100))

        e1, e2 = _compute_event_types(docs)
        try:
            multiprocessing.Process(target=update_info, args=(docs, e1, e2, colname)).start()
        except Exception as err:
            print("Failed to spawn update_info:", err)

        docs = _apply_georgia_filter_docs(docs, country_code, 'int')
        if len(docs) != len(e1):
            e1, e2 = _compute_event_types(docs)

        counts_all = _count_events_from_types(e1, e2)
        for et, v in counts_all.items():
            df_combined.loc[date, et] = v

        yes_idx, no_idx = _split_related(docs)
        e1_yes, e2_yes = _subset(e1, yes_idx), _subset(e2, yes_idx)
        e1_no,  e2_no  = _subset(e1, no_idx),  _subset(e2, no_idx)
        counts_yes = _count_events_from_types(e1_yes, e2_yes)
        counts_no  = _count_events_from_types(e1_no,  e2_no)

        for et, v in counts_yes.items():
            df_rel.loc[date, et] = v
        for et, v in counts_no.items():
            df_nonrel.loc[date, et] = v

    # write original per-source raw outputs (unchanged)
    base = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/Raw_By_Source/{country_name}/{today.year}_{today.month}_{today.day}/"
    Path(base + "Combined/").mkdir(parents=True, exist_ok=True)
    Path(base + "Civic_Related/").mkdir(parents=True, exist_ok=True)
    Path(base + "Non_Civic_Related/").mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(os.path.join(base, "Combined", f"{domain}.csv"))
    df_rel.to_csv(os.path.join(base, "Civic_Related", f"{domain}.csv"))
    df_nonrel.to_csv(os.path.join(base, "Non_Civic_Related", f"{domain}.csv"))

    return df_combined, df_rel, df_nonrel

# === Per-domain denominators (ALL local articles; no civic filters) ===
def denom_domain_loc(uri, domain, country_name, country_code):
    dbm = MongoClient(uri).ml4p
    df = _prepare_denom_eom()
    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"
        # lenient
        q_non_en = {'source_domain': domain, 'include': True, 'language': {'$ne': 'en'},
                    '$or': [{f'cliff_locations.{country_code}': {'$exists': True}},
                            {'cliff_locations': {}},
                            {'cliff_locations': {'$exists': False}}]}
        q_en     = {'source_domain': domain, 'include': True, 'language': 'en',
                    '$or': [{f'en_cliff_locations.{country_code}': {'$exists': True}},
                            {'en_cliff_locations': {}},
                            {'en_cliff_locations': {'$exists': False}}]}
        if country_code == 'GEO':
            proj = {'title_translated':1, 'maintext_translated':1, '_id':0}
            docs = list(dbm[colname].find(q_non_en, projection=proj, batch_size=200)) + \
                   list(dbm[colname].find(q_en,     projection=proj, batch_size=200))
            cnt = 0
            for d in docs:
                if check_georgia_text(d.get('title_translated',''), 'loc') and \
                   check_georgia_text(d.get('maintext_translated',''), 'loc'):
                    cnt += 1
        else:
            cnt = dbm[colname].count_documents(q_non_en) + dbm[colname].count_documents(q_en)
        df.loc[date, 'denom_total_local'] = cnt
    return df

def denom_domain_int(uri, domain, country_name, country_code):
    dbm = MongoClient(uri).ml4p
    df = _prepare_denom_eom()
    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"
        # strict
        q_non_en = {'source_domain': domain, 'include': True, 'language': {'$ne': 'en'},
                    f'cliff_locations.{country_code}': {'$exists': True}}
        q_en     = {'source_domain': domain, 'include': True, 'language': 'en',
                    f'en_cliff_locations.{country_code}': {'$exists': True}}
        if country_code == 'GEO':
            proj = {'title_translated':1, 'maintext_translated':1, '_id':0}
            docs = list(dbm[colname].find(q_non_en, projection=proj, batch_size=200)) + \
                   list(dbm[colname].find(q_en,     projection=proj, batch_size=200))
            cnt = 0
            for d in docs:
                if check_georgia_text(d.get('title_translated',''), 'int') and \
                   check_georgia_text(d.get('maintext_translated',''), 'int'):
                    cnt += 1
        else:
            cnt = dbm[colname].count_documents(q_non_en) + dbm[colname].count_documents(q_en)
        df.loc[date, 'denom_total_local'] = cnt
    return df

# === Utilities to sum monthly frames ===
def _sum_frames(frames):
    if not frames:
        return _prepare_df_eom()
    out = pd.concat(frames).groupby(level=0).sum(numeric_only=True)
    out['year'] = out.index.year
    out['month'] = out.index.month
    return out

def _sum_denoms(frames):
    if not frames:
        return _prepare_denom_eom()
    out = pd.concat(frames).groupby(level=0).sum(numeric_only=True)
    out['year'] = out.index.year
    out['month'] = out.index.month
    return out

# === Country orchestrator ===
def process_country(uri, country_name, country_code, num_cpus=10):
    dbm = MongoClient(uri).ml4p

    # Source sets (no ENV_)
    if country_code == 'XKX':
        local_sources = [d['source_domain'] for d in dbm['sources'].find({'primary_location': {'$in':[country_code]}, 'include': True})] + ['balkaninsight.com']
        int_sources   = [d['source_domain'] for d in dbm['sources'].find({'major_international': True, 'include': True})]
        regional_src  = [d['source_domain'] for d in dbm['sources'].find({'major_regional': True, 'include': True}) if d['source_domain']!='balkaninsight.com']
    elif country_code == 'KAZ':
        local_sources = [d['source_domain'] for d in dbm['sources'].find({'primary_location': {'$in':[country_code]}, 'include': True})] + ['kaztag.kz']
        int_sources   = [d['source_domain'] for d in dbm['sources'].find({'major_international': True, 'include': True})]
        regional_src  = [d['source_domain'] for d in dbm['sources'].find({'major_regional': True, 'include': True})]
    else:
        local_sources = [d['source_domain'] for d in dbm['sources'].find({'primary_location': {'$in':[country_code]}, 'include': True})]
        int_sources   = [d['source_domain'] for d in dbm['sources'].find({'major_international': True, 'include': True})]
        regional_src  = [d['source_domain'] for d in dbm['sources'].find({'major_regional': True, 'include': True})]

    # Dedup and avoid overlaps between sets
    local_sources = sorted(set(local_sources))
    int_sources   = sorted(set(int_sources))
    regional_src  = sorted(set(regional_src))

    print(f"[{country_code}] Local: {len(local_sources)}, INT: {len(int_sources)}, REG: {len(regional_src)}")

    # Run per-domain civic counters (numerators)
    loc_args = [(uri, d, country_name, country_code) for d in local_sources]
    int_args = [(uri, d, country_name, country_code) for d in int_sources]
    reg_args = [(uri, d, country_name, country_code) for d in regional_src]

    loc_results = p_umap(lambda a: count_domain_loc_merged(*a), loc_args, num_cpus=num_cpus) if loc_args else []
    int_results = p_umap(lambda a: count_domain_int_merged(*a), int_args, num_cpus=num_cpus) if int_args else []
    reg_results = p_umap(lambda a: count_domain_int_merged(*a), reg_args, num_cpus=num_cpus) if reg_args else []

    # Split tuples into dicts domain -> frames
    domain_to_comb, domain_to_rel, domain_to_nonrel = {}, {}, {}
    for d, (dfc, dfr, dfn) in zip(local_sources, loc_results): domain_to_comb[d], domain_to_rel[d], domain_to_nonrel[d] = dfc, dfr, dfn
    for d, (dfc, dfr, dfn) in zip(int_sources, int_results):  domain_to_comb[d], domain_to_rel[d], domain_to_nonrel[d] = dfc, dfr, dfn
    for d, (dfc, dfr, dfn) in zip(regional_src, reg_results): domain_to_comb[d], domain_to_rel[d], domain_to_nonrel[d] = dfc, dfr, dfn

    # Denominators per domain (ALL local articles; no civic filters)
    denom_loc = p_umap(lambda a: denom_domain_loc(*a), loc_args, num_cpus=num_cpus) if loc_args else []
    denom_int = p_umap(lambda a: denom_domain_int(*a), int_args, num_cpus=num_cpus) if int_args else []
    denom_reg = p_umap(lambda a: denom_domain_int(*a), reg_args, num_cpus=num_cpus) if reg_args else []

    # Country-level raw (sum across sources) for each slice
    country_comb_raw   = _sum_frames(list(domain_to_comb.values()))
    country_rel_raw    = _sum_frames(list(domain_to_rel.values()))
    country_nonrel_raw = _sum_frames(list(domain_to_nonrel.values()))

    # Country-level denominators (sum across sources)
    country_denom = _sum_denoms(denom_loc + denom_int + denom_reg)
    denom_series = country_denom['denom_total_local'].astype('float64')
    denom_series = denom_series.mask(denom_series == 0, np.nan)

    # Add _norm columns at country-level
    for et in civic_category_labels:
        country_comb_raw[et + '_norm']   = country_comb_raw[et].astype('float64') / denom_series
        country_rel_raw[et + '_norm']    = country_rel_raw[et].astype('float64') / denom_series
        country_nonrel_raw[et + '_norm'] = country_nonrel_raw[et].astype('float64') / denom_series

    # Write country-level (raw + _norm)
    out_final_base = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/Final_Aggregated/{country_name}/{today.year}_{today.month}_{today.day}/"
    Path(out_final_base).mkdir(parents=True, exist_ok=True)
    country_comb_raw.sort_index().to_csv(os.path.join(out_final_base, f"{country_name}_Combined.csv"))
    country_rel_raw.sort_index().to_csv(os.path.join(out_final_base, f"{country_name}_Civic_Related.csv"))
    country_nonrel_raw.sort_index().to_csv(os.path.join(out_final_base, f"{country_name}_Non_Civic_Related.csv"))

    # Write by-source normalized (divide every numeric column except year/month by the SAME country-month denominator)
    out_by_source_base = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/Normalized_By_Source/{country_name}/{today.year}_{today.month}_{today.day}/"
    for sub in ["Combined", "Civic_Related", "Non_Civic_Related"]:
        Path(os.path.join(out_by_source_base, sub)).mkdir(parents=True, exist_ok=True)

    def _norm_and_write(df_map, subdir):
        for domain, df in df_map.items():
            df_norm = df.copy()
            aligned = denom_series.reindex(df_norm.index)
            numeric_cols = [c for c in df_norm.columns if pd.api.types.is_numeric_dtype(df_norm[c]) and c not in ('year','month')]
            df_norm[numeric_cols] = df_norm[numeric_cols].astype('float64').div(aligned, axis=0)
            df_norm.sort_index().to_csv(os.path.join(out_by_source_base, subdir, f"{domain}.csv"))

    _norm_and_write(domain_to_comb,   "Combined")
    _norm_and_write(domain_to_rel,    "Civic_Related")
    _norm_and_write(domain_to_nonrel, "Non_Civic_Related")

# === Git helper ===
def run_git_commands(commit_message):
    try:
        subprocess.run("git add *.py", shell=True, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

# === Main ===
if __name__ == '__main__':
    slp = False
    if slp:
        t = 7200
        print(f"start sleeping for {t/60} mins")
        time.sleep(t)

    all_countries = [
        ('Albania','ALB'),('Benin','BEN'),('Colombia','COL'),('Ecuador','ECU'),
        ('Ethiopia','ETH'),('Georgia','GEO'),('Kenya','KEN'),('Paraguay','PRY'),
        ('Mali','MLI'),('Morocco','MAR'),('Nigeria','NGA'),('Serbia','SRB'),
        ('Senegal','SEN'),('Tanzania','TZA'),('Uganda','UGA'),('Ukraine','UKR'),
        ('Zimbabwe','ZWE'),('Mauritania','MRT'),('Zambia','ZMB'),('Kosovo','XKX'),
        ('Niger','NER'),('Jamaica','JAM'),('Honduras','HND'),('Philippines','PHL'),
        ('Ghana','GHA'),('Rwanda','RWA'),('Guatemala','GTM'),('Belarus','BLR'),
        ('Cambodia','KHM'),('DR Congo','COD'),('Turkey','TUR'),('Bangladesh','BGD'),
        ('El Salvador','SLV'),('South Africa','ZAF'),('Tunisia','TUN'),('Indonesia','IDN'),
        ('Nicaragua','NIC'),('Angola','AGO'),('Armenia','ARM'),('Sri Lanka','LKA'),
        ('Malaysia','MYS'),('Cameroon','CMR'),('Hungary','HUN'),('Malawi','MWI'),
        ('Uzbekistan','UZB'),('India','IND'),('Mozambique','MOZ'),('Azerbaijan','AZE'),
        ('Kyrgyzstan','KGZ'),('Moldova','MDA'),('Kazakhstan','KAZ'),('Peru','PER'),
        ('Algeria','DZA'),('Macedonia','MKD'),('South Sudan','SSD'),('Liberia','LBR'),
        ('Pakistan','PAK'),('Nepal','NPL'),('Namibia','NAM'),('Burkina Faso','BFA'),
        ('Dominican Republic','DOM'),('Timor Leste','TLS'),('Solomon Islands','SLB'),
        ('Costa Rica','CRI'),('Panama','PAN'),('Mexico','MEX')
    ]

    # Edit this to run a batch
    countries_needed = [
        # 'ARM','BLR','KGZ','MKD','MDA','SRB','SLV','DOM','NIC','PRY','KHM','LKA','LBR','ZMB','ZWE','ECU','ALB','MEX','PHL','UZB','AGO','XKX','MKD','BFA','CMR'
        # 'AZE','GEO','HUN','UKR'
        # 'IND','IDN','HUN','AZE','CRI','ECU','ETH','BGD','COL','DZA','SRB'
        # 'ALB', 'BEN', 'COL', 'ECU', 'ETH', 'GEO', 'KEN', 'PRY', 'MLI', 'MAR', 'NGA', 'SRB', 'SEN', 'TZA', 'UGA', 'UKR', 'ZWE', 'MRT', 'ZMB', 'XKX', 'NER', 'JAM', 'HND', 'PHL', 'GHA', 'RWA', 'GTM', 'BLR', 'KHM', 'COD', 'TUR', 'BGD', 'SLV', 'ZAF', 'TUN', 'IDN', 'NIC', 'AGO', 'ARM', 'LKA', 'MYS', 'CMR', 'HUN', 'MWI', 'UZB', 'IND', 'MOZ', 'AZE', 'KGZ', 'MDA', 'KAZ', 'PER', 'DZA', 'MKD', 'SSD', 'LBR', 'PAK', 'NPL', 'NAM', 'BFA', 'DOM', 'TLS', 'SLB', 'CRI', 'PAN'
        # 'MEX'
        # 'KAZ','MWI','MRT','JAM','NAM','NGA','MYS','MAR','NPL','NER','PAK'
         'LBR','ZWE','ARM','ZMB','BLR','SLV'
        ]
    countries = [(n,c) for (n,c) in all_countries if c in countries_needed]

    for country_name, country_code in countries:
        print('Starting:', country_name)
        process_country(uri, country_name, country_code, num_cpus=10)
        try:
            commit_message = f"civic counts + normalized (by-source & final) ({country_code})"
            run_git_commands(commit_message)
        except Exception:
            pass

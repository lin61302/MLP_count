#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAI merged (China + Russia + Combined) — single pass per domain/month.

This file preserves the original **country pairs**, **source selection**, and the
way the batch is run by **keeping the original `__main__` orchestration verbatim**
(from RAI_new.py). The only change is that the underlying `count_domain_loc` and
`count_domain_int` now compute **all three** outputs (China, Russia, Combined)
in one DB pass per (domain, month).

Outputs and DB fields preserved:
- China:   event_type_RAI_new_China -> CSV under .../Counts_RAI_New/.../China/
- Russia:  event_type_RAI_new_Russia -> CSV under .../Counts_RAI_New/.../Russia/
- Combined: event_type_RAI_new + RAI_new_keywords (+ Country_Georgia) -> CSV under .../Combined/
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

# ---------- Setup (same environment and DB) ----------
load_dotenv()
today = pd.Timestamp.now()
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

# ---------- Keyword sources (exact paths) ----------
__russiapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Russia_RAI_keywords_0730.xlsx'
__chinapath__  = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/China_RAI_keywords_0730.xlsx'
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

# Load sheets
russia_df = pd.read_excel(__russiapath__)
china_df  = pd.read_excel(__chinapath__)

ru = russia_df['CompanyName'].str.strip()
ch = china_df['CompanyName'].str.strip()
ru_ind = russia_df['alphabet_connect']
ch_ind = china_df['alphabet_connect']

# ---------- Regex compilers ----------
def compile_regex(keyword_list, boundary_list):
    patterns = []
    for kw, boundary in zip(keyword_list, boundary_list):
        s = str(kw).strip()
        if not s:
            continue
        if boundary:
            patterns.append(f"(?<![a-zA-Z]){re.escape(s)}(?![a-zA-Z])")
        else:
            patterns.append(re.escape(s))
    if not patterns:
        patterns = ["(?!x)x"]
    return re.compile('|'.join(patterns), flags=re.IGNORECASE)

# China-only and Russia-only patterns (match originals)
rai_re_china   = re.compile('|'.join(ch), flags=re.IGNORECASE)
title_re_china = re.compile(r'(china|chinese)', flags=re.IGNORECASE)

rai_re_russia   = re.compile('|'.join(ru), flags=re.IGNORECASE)
title_re_russia = re.compile(r'(russia|russian)', flags=re.IGNORECASE)

# Combined boundary-aware union
rai_re_combined   = compile_regex(pd.concat([ru, ch]), pd.concat([ru_ind, ch_ind]).tolist())
title_re_combined = re.compile(r'(china|chinese|russia|russian)', flags=re.IGNORECASE)

# Georgia filters (boundary on every term)
geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip()
g_loc = geo_loc['CompanyName'].str.strip()
g_int_filter = compile_regex(g_int, [True]*len(g_int))
g_loc_filter = compile_regex(g_loc, [True]*len(g_loc))

def check_georgia(doc, scope):
    try:
        if scope == 'loc':
            return not g_loc_filter.search(doc)
        else:
            return not g_int_filter.search(doc)
    except Exception:
        return True

# ---------- Event list ----------
try:
    events = [k for k in db.models.find_one({'model_name': 'RAI_new'}).get('event_type_nums').keys()]
except Exception:
    events = []

# ---------- Utilities ----------
def _prepare_df():
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month
    for et in events:
        df[et] = 0
    return df

def _project():
    return {
        '_id': 1, 'RAI_new': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'language': 1, 'cliff_locations': 1, 'en_cliff_locations': 1
    }

def _safe_colname(doc):
    try:
        return f"articles-{doc['date_publish'].year}-{doc['date_publish'].month}"
    except Exception:
        dd = dateparser.parse(doc.get('date_publish')).replace(tzinfo=None)
        return f"articles-{dd.year}-{dd.month}"

def _doc_passes(pattern_re, title_re, title, main2k):
    try:
        return bool(pattern_re.search(main2k)) or bool(pattern_re.search(title)) or bool(title_re.search(title))
    except Exception:
        return False

def _count_events(docs, event_types, country_code, scope):
    counts = {et: 0 for et in events}
    N = len(docs)
    for et in events:
        if et == '-999':
            cnt = sum(1 for i in range(N) if event_types[i] == '-999')
        else:
            cnt = 0
            for i, d in enumerate(docs):
                try:
                    if d['RAI_new'].get('result') == et and event_types[i] == et:
                        cnt += 1
                except Exception:
                    pass

        if country_code == 'GEO' and cnt > 0:
            def cond(i):
                try:
                    text_ok = check_georgia(docs[i]['maintext_translated'][:2000], scope) and \
                              check_georgia(docs[i]['title_translated'], scope)
                except Exception:
                    text_ok = True
                if et == '-999':
                    return (event_types[i] == '-999') and text_ok
                else:
                    try:
                        return (docs[i]['RAI_new'].get('result') == et and event_types[i] == et) and text_ok
                    except Exception:
                        return False
            cnt = sum(cond(i) for i in range(N))
        counts[et] = cnt
    return counts

def _write_csv(df, country_name, domain, bucket):
    base = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI_New/{country_name}/{today.year}_{today.month}_{today.day}/{bucket}/"
    Path(base).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(base, f"{domain}.csv"))

# ---------- DB Updates (preserve field names) ----------
def _update_info_china(docs, e_ch):
    dbl = MongoClient(uri).ml4p
    for nn, _doc in enumerate(docs):
        try:
            col = _safe_colname(_doc)
            dbl[col].update_one({'_id': _doc['_id']}, {'$set': {'event_type_RAI_new_China': e_ch[nn]}})
        except Exception:
            pass

def _update_info_russia(docs, e_ru):
    dbl = MongoClient(uri).ml4p
    for nn, _doc in enumerate(docs):
        try:
            col = _safe_colname(_doc)
            dbl[col].update_one({'_id': _doc['_id']}, {'$set': {'event_type_RAI_new_Russia': e_ru[nn]}})
        except Exception:
            pass

def _update_info_combined(docs, e_cb, kw_cb):
    dbl = MongoClient(uri).ml4p
    for nn, _doc in enumerate(docs):
        try:
            col = _safe_colname(_doc)
            dbl[col].update_one({'_id': _doc['_id']}, {'$set': {
                'event_type_RAI_new': e_cb[nn],
                'RAI_new_keywords': kw_cb[nn]
            }})
        except Exception:
            pass

def _update_country_georgia_flag(docs, scope):
    dbl = MongoClient(uri).ml4p
    for _doc in docs:
        try:
            col = _safe_colname(_doc)
            try:
                is_yes = check_georgia(_doc['maintext_translated'], scope) and check_georgia(_doc['title_translated'], scope)
            except Exception:
                is_yes = True
            dbl[col].update_one({'_id': _doc['_id']}, {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}})
        except Exception:
            pass

# ---------- Merged counters with identical signatures ----------
def count_domain_loc(uri, domain, country_name, country_code):
    db_local = MongoClient(uri).ml4p
    df_ch = _prepare_df()
    df_ru = _prepare_df()
    df_cb = _prepare_df()

    projection = _project()

    for date in df_ch.index:
        colname = f'articles-{date.year}-{date.month}'
        # Two queries: non-EN with cliff_locations, EN with en_cliff_locations (OR exists or {})
        cur1 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI_new': {'$exists': True, '$ne': None},
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
        cur2 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI_new': {'$exists': True, '$ne': None},
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

        docs = list(cur1) + list(cur2)
        if not docs:
            continue

        N = len(docs)
        e_ch, e_ru, e_cb = [], [], []
        kw_cb = [[] for _ in range(N)]

        for i, d in enumerate(docs):
            title = d.get('title_translated', '') or ''
            main_snip = (d.get('maintext_translated', '') or '')[:2000]

            # China
            if _doc_passes(rai_re_china, title_re_china, title, main_snip):
                e_ch.append(d.get('RAI_new', {}).get('result', '-999'))
            else:
                e_ch.append('-999')

            # Russia
            if _doc_passes(rai_re_russia, title_re_russia, title, main_snip):
                e_ru.append(d.get('RAI_new', {}).get('result', '-999'))
            else:
                e_ru.append('-999')

            # Combined (+ keywords)
            if _doc_passes(rai_re_combined, title_re_combined, title, main_snip):
                e_cb.append(d.get('RAI_new', {}).get('result', '-999'))
                try:
                    kw_cb[i].extend(rai_re_combined.findall(f"{title} {main_snip}"))
                except Exception:
                    pass
            else:
                e_cb.append('-999')

        kw_cb = [list(set(k)) for k in kw_cb]

        # Async updates
        try:
            p1 = multiprocessing.Process(target=_update_info_china, args=(docs, e_ch))
            p2 = multiprocessing.Process(target=_update_info_russia, args=(docs, e_ru))
            p3 = multiprocessing.Process(target=_update_info_combined, args=(docs, e_cb, kw_cb))
            p1.start(); p2.start(); p3.start()
            p1.join(); p2.join(); p3.join()
        except Exception as e:
            print("update_info spawn error:", e)

        # Combined script sets Country_Georgia
        if country_code == 'GEO':
            try:
                _update_country_georgia_flag(docs, 'loc')
            except Exception:
                pass

        # Counts
        counts_ch = _count_events(docs, e_ch, country_code, 'loc')
        counts_ru = _count_events(docs, e_ru, country_code, 'loc')
        counts_cb = _count_events(docs, e_cb, country_code, 'loc')

        for et, v in counts_ch.items():
            df_ch.loc[date, et] = v
        for et, v in counts_ru.items():
            df_ru.loc[date, et] = v
        for et, v in counts_cb.items():
            df_cb.loc[date, et] = v

    _write_csv(df_ch, country_name, domain, 'China')
    _write_csv(df_ru, country_name, domain, 'Russia')
    _write_csv(df_cb, country_name, domain, 'Combined')

def count_domain_int(uri, domain, country_name, country_code):
    db_local = MongoClient(uri).ml4p
    df_ch = _prepare_df()
    df_ru = _prepare_df()
    df_cb = _prepare_df()

    projection = _project()

    for date in df_ch.index:
        colname = f'articles-{date.year}-{date.month}'
        # International: must explicitly include the country code in locations
        cur1 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'RAI_new': {'$exists': True, '$ne': None},
                'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
                'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
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
                'RAI_new': {'$exists': True, '$ne': None},
                'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
                'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
                'language': 'en',
                f'en_cliff_locations.{country_code}': {'$exists': True}
            },
            projection=projection,
            batch_size=100
        )

        docs = list(cur1) + list(cur2)
        if not docs:
            continue

        N = len(docs)
        e_ch, e_ru, e_cb = [], [], []
        kw_cb = [[] for _ in range(N)]

        for i, d in enumerate(docs):
            title = d.get('title_translated', '') or ''
            main_snip = (d.get('maintext_translated', '') or '')[:2000]

            # China
            if _doc_passes(rai_re_china, title_re_china, title, main_snip):
                e_ch.append(d.get('RAI_new', {}).get('result', '-999'))
            else:
                e_ch.append('-999')

            # Russia
            if _doc_passes(rai_re_russia, title_re_russia, title, main_snip):
                e_ru.append(d.get('RAI_new', {}).get('result', '-999'))
            else:
                e_ru.append('-999')

            # Combined (+ keywords)
            if _doc_passes(rai_re_combined, title_re_combined, title, main_snip):
                e_cb.append(d.get('RAI_new', {}).get('result', '-999'))
                try:
                    kw_cb[i].extend(rai_re_combined.findall(f"{title} {main_snip}"))
                except Exception:
                    pass
            else:
                e_cb.append('-999')

        kw_cb = [list(set(k)) for k in kw_cb]

        # Async updates
        try:
            p1 = multiprocessing.Process(target=_update_info_china, args=(docs, e_ch))
            p2 = multiprocessing.Process(target=_update_info_russia, args=(docs, e_ru))
            p3 = multiprocessing.Process(target=_update_info_combined, args=(docs, e_cb, kw_cb))
            p1.start(); p2.start(); p3.start()
            p1.join(); p2.join(); p3.join()
        except Exception as e:
            print("update_info spawn error:", e)

        # Combined script sets Country_Georgia
        if country_code == 'GEO':
            try:
                _update_country_georgia_flag(docs, 'int')
            except Exception:
                pass

        # Counts
        counts_ch = _count_events(docs, e_ch, country_code, 'int')
        counts_ru = _count_events(docs, e_ru, country_code, 'int')
        counts_cb = _count_events(docs, e_cb, country_code, 'int')

        for et, v in counts_ch.items():
            df_ch.loc[date, et] = v
        for et, v in counts_ru.items():
            df_ru.loc[date, et] = v
        for et, v in counts_cb.items():
            df_cb.loc[date, et] = v

    _write_csv(df_ch, country_name, domain, 'China')
    _write_csv(df_ru, country_name, domain, 'Russia')
    _write_csv(df_cb, country_name, domain, 'Combined')

# ---------- Git helper (unchanged) ----------
def run_git_commands(commit_message):
    try:
        subprocess.run("git add *.py", shell=True, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
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
        ('Panama','PAN'),
        ('Mexico','MEX')
    ]
    

    countries_needed = [
        'BEN', 'UKR', 'GEO', 'GTM','NIC', 'PRY'
            #  'MOZ','COD','SSD','ZWE','GHA','KHM'
                         ]
    # countries_needed = ['PHL','BFA','AGO','AZE','MWI','BLR','BGD','HUN','XKX','MYS']

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
        commit_message = f"RAI New count ({countries_added}) update"
        # run_git_commands(commit_message)


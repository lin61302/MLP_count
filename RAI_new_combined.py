#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAI counters with country-level and by-source normalization and persistent China/Russia flags.

Normalization denominator per month = ALL articles considered local
across ALL sources (locals lenient, int/reg strict), with no RAI filters.

Outputs:
- New country-level (norm):
  Counts_RAI_New/Final_Aggregated/{country}/{YYYY_M_D}/{country}_{China|Russia|Combined}.csv
- New by-source normalized (divide every numeric col except year/month by country-month denominator):
  Counts_RAI_New/Normalized_By_Source/{country}/{YYYY_M_D}/{China|Russia|Combined}/{domain}.csv
- New by-source raw:
  Counts_RAI_New/Raw_By_Source/{country}/{YYYY_M_D}/{China|Russia|Combined}/{domain}.csv
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

# ---------- Setup ----------
load_dotenv()
today = pd.Timestamp.now()
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

# ---------- Keyword sources ----------
__russiapath__ = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/Russia_RAI_keywords_0730.xlsx'
__chinapath__  = '/home/ml4p/Dropbox/Dropbox/ML for Peace/Acronyms/China_RAI_keywords_0730.xlsx'
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

# Load sheets
russia_df = pd.read_excel(__russiapath__)
china_df  = pd.read_excel(__chinapath__)

ru = russia_df['CompanyName'].str.strip().fillna('')
ch = china_df['CompanyName'].str.strip().fillna('')
ru_ind = russia_df.get('alphabet_connect', pd.Series([True]*len(ru))).fillna(True)
ch_ind = china_df.get('alphabet_connect', pd.Series([True]*len(ch))).fillna(True)

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

# China/Russia patterns (keep parity with prior behavior; union is boundary-aware)
rai_re_china    = re.compile('|'.join([re.escape(s) for s in ch if s]), flags=re.IGNORECASE)
title_re_china  = re.compile(r'(china|chinese)', flags=re.IGNORECASE)

rai_re_russia   = re.compile('|'.join([re.escape(s) for s in ru if s]), flags=re.IGNORECASE)
title_re_russia = re.compile(r'(russia|russian)', flags=re.IGNORECASE)

rai_re_combined   = compile_regex(pd.concat([ru, ch]), pd.concat([ru_ind, ch_ind]).tolist())
title_re_combined = re.compile(r'(china|chinese|russia|russian)', flags=re.IGNORECASE)

# Georgia filters (boundary on every term)
geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip().fillna('').tolist()
g_loc = geo_loc['CompanyName'].str.strip().fillna('').tolist()
g_int_filter = compile_regex(g_int, [True]*len(g_int))
g_loc_filter = compile_regex(g_loc, [True]*len(g_loc))

def check_georgia_text(text, mode):
    try:
        if mode == 'loc':
            return not g_loc_filter.search(text or "")
        else:
            return not g_int_filter.search(text or "")
    except Exception:
        return True

# ---------- Event list ----------
try:
    events = [k for k in db.models.find_one({'model_name': 'RAI_new'}).get('event_type_nums').keys()]
except Exception:
    events = []

# ---------- Utilities ----------
def _prepare_df_eom():
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-01-01', today + pd.Timedelta(31, 'd'), freq='M')
    df = df.set_index('date')
    df['year'] = df.index.year
    df['month'] = df.index.month
    for et in events:
        df[et] = 0
    return df

def _prepare_denom_eom():
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-01-01', today + pd.Timedelta(31,'d'), freq='M')
    df = df.set_index('date')
    df['year'] = df.index.year
    df['month'] = df.index.month
    df['denom_total_local'] = 0
    return df

def _project_base():
    # include flags so we can avoid re-running keywords next time
    return {
        '_id': 1, 'RAI_new': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'language': 1, 'cliff_locations': 1, 'en_cliff_locations': 1,
        'RAI_is_china_related': 1, 'RAI_is_russia_related': 1
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

def _get_or_update_flags(dbl, doc, title, main_snip):
    """
    Return (is_china, is_russia). If flags missing, compute via regex and persist.
    """
    need_update = False
    is_ch = doc.get('RAI_is_china_related', None)
    is_ru = doc.get('RAI_is_russia_related', None)

    if is_ch is None:
        is_ch = _doc_passes(rai_re_china, title_re_china, title, main_snip)
        need_update = True
    if is_ru is None:
        is_ru = _doc_passes(rai_re_russia, title_re_russia, title, main_snip)
        need_update = True

    if need_update:
        try:
            col = _safe_colname(doc)
            dbl[col].update_one(
                {'_id': doc['_id']},
                {'$set': {
                    'RAI_is_china_related': bool(is_ch),
                    'RAI_is_russia_related': bool(is_ru)
                }}
            )
        except Exception:
            pass

    return bool(is_ch), bool(is_ru)

def _count_events(docs, chosen_event_types, country_code, scope):
    """
    Count events restricting to documents selected for a bucket (China / Russia / Combined).
    chosen_event_types[i] is '-999' if doc i is excluded for the bucket; otherwise the predicted class.
    """
    counts = {et: 0 for et in events}
    N = len(docs)
    for et in events:
        if et == '-999':
            cnt = sum(1 for i in range(N) if chosen_event_types[i] == '-999')
        else:
            def ok(i):
                try:
                    return (docs[i]['RAI_new'].get('result') == et) and (chosen_event_types[i] == et)
                except Exception:
                    return False

            if country_code == 'GEO':
                def ok_geo(i):
                    if not ok(i):
                        return False
                    try:
                        return check_georgia_text(docs[i].get('maintext_translated',''), scope) and \
                               check_georgia_text(docs[i].get('title_translated',''), scope)
                    except Exception:
                        return True
                cnt = sum(ok_geo(i) for i in range(N))
            else:
                cnt = sum(ok(i) for i in range(N))
        counts[et] = cnt
    return counts

def _write_raw_csv(df, country_name, domain, bucket):
    base = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI_New/Raw_By_Source/{country_name}/{today.year}_{today.month}_{today.day}/{bucket}/"
    Path(base).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(base, f"{domain}.csv"))

# ---------- Per-domain counting (locals = lenient; int/reg = strict) ----------
def count_domain_loc(uri, domain, country_name, country_code):
    dbl = MongoClient(uri).ml4p
    df_ch = _prepare_df_eom()
    df_ru = _prepare_df_eom()
    df_cb = _prepare_df_eom()
    projection = _project_base()

    for date in df_ch.index:
        colname = f'articles-{date.year}-{date.month}'

        q_non_en = {
            'source_domain': domain, 'include': True,
            'RAI_new': {'$exists': True, '$ne': None},
            'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            'language': {'$ne': 'en'},
            '$or': [
                {f'cliff_locations.{country_code}': {'$exists': True}},
                {'cliff_locations': {}}
            ]
        }
        q_en = {
            'source_domain': domain, 'include': True,
            'RAI_new': {'$exists': True, '$ne': None},
            'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            'language': 'en',
            '$or': [
                {f'en_cliff_locations.{country_code}': {'$exists': True}},
                {'en_cliff_locations': {}}
            ]
        }

        docs = list(dbl[colname].find(q_non_en, projection=projection, batch_size=100)) + \
               list(dbl[colname].find(q_en,     projection=projection, batch_size=100))
        if not docs:
            continue

        N = len(docs)
        e_ch, e_ru, e_cb = [], [], []

        for i, d in enumerate(docs):
            title = d.get('title_translated', '') or ''
            main_snip = (d.get('maintext_translated', '') or '')[:2000]

            # read or compute flags once, persist if missing
            is_ch, is_ru = _get_or_update_flags(dbl, d, title, main_snip)
            is_cb = is_ch or is_ru

            # Assign class for each bucket (or '-999' if excluded)
            et = (d.get('RAI_new') or {}).get('result', '-999')
            e_ch.append(et if is_ch else '-999')
            e_ru.append(et if is_ru else '-999')
            e_cb.append(et if is_cb else '-999')

        # Country_Georgia tagging mirror (side-effect)
        if country_code == 'GEO':
            try:
                for d in docs:
                    col = _safe_colname(d)
                    try:
                        is_yes = check_georgia_text(d.get('maintext_translated',''), 'loc') and \
                                 check_georgia_text(d.get('title_translated',''), 'loc')
                    except Exception:
                        is_yes = True
                    dbl[col].update_one({'_id': d['_id']}, {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}})
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

    # keep original raw outputs
    _write_raw_csv(df_ch, country_name, domain, 'China')
    _write_raw_csv(df_ru, country_name, domain, 'Russia')
    _write_raw_csv(df_cb, country_name, domain, 'Combined')
    return df_ch, df_ru, df_cb

def count_domain_int(uri, domain, country_name, country_code):
    dbl = MongoClient(uri).ml4p
    df_ch = _prepare_df_eom()
    df_ru = _prepare_df_eom()
    df_cb = _prepare_df_eom()
    projection = _project_base()

    for date in df_ch.index:
        colname = f'articles-{date.year}-{date.month}'

        q_non_en = {
            'source_domain': domain, 'include': True,
            'RAI_new': {'$exists': True, '$ne': None},
            'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            'language': {'$ne': 'en'},
            f'cliff_locations.{country_code}': {'$exists': True}
        }
        q_en = {
            'source_domain': domain, 'include': True,
            'RAI_new': {'$exists': True, '$ne': None},
            'title_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            'maintext_translated': {'$exists': True, '$ne': '', '$type': 'string'},
            'language': 'en',
            f'en_cliff_locations.{country_code}': {'$exists': True}
        }

        docs = list(dbl[colname].find(q_non_en, projection=projection, batch_size=100)) + \
               list(dbl[colname].find(q_en,     projection=projection, batch_size=100))
        if not docs:
            continue

        N = len(docs)
        e_ch, e_ru, e_cb = [], [], []

        for i, d in enumerate(docs):
            title = d.get('title_translated', '') or ''
            main_snip = (d.get('maintext_translated', '') or '')[:2000]

            is_ch, is_ru = _get_or_update_flags(dbl, d, title, main_snip)
            is_cb = is_ch or is_ru
            et = (d.get('RAI_new') or {}).get('result', '-999')

            e_ch.append(et if is_ch else '-999')
            e_ru.append(et if is_ru else '-999')
            e_cb.append(et if is_cb else '-999')

        if country_code == 'GEO':
            try:
                for d in docs:
                    col = _safe_colname(d)
                    try:
                        is_yes = check_georgia_text(d.get('maintext_translated',''), 'int') and \
                                 check_georgia_text(d.get('title_translated',''), 'int')
                    except Exception:
                        is_yes = True
                    dbl[col].update_one({'_id': d['_id']}, {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}})
            except Exception:
                pass

        counts_ch = _count_events(docs, e_ch, country_code, 'int')
        counts_ru = _count_events(docs, e_ru, country_code, 'int')
        counts_cb = _count_events(docs, e_cb, country_code, 'int')

        for et, v in counts_ch.items():
            df_ch.loc[date, et] = v
        for et, v in counts_ru.items():
            df_ru.loc[date, et] = v
        for et, v in counts_cb.items():
            df_cb.loc[date, et] = v

    _write_raw_csv(df_ch, country_name, domain, 'China')
    _write_raw_csv(df_ru, country_name, domain, 'Russia')
    _write_raw_csv(df_cb, country_name, domain, 'Combined')
    return df_ch, df_ru, df_cb

# ---------- Denominators (ALL local articles; no RAI filters) ----------
def denom_domain_loc(uri, domain, country_name, country_code):
    dbm = MongoClient(uri).ml4p
    df = _prepare_denom_eom()
    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"
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

# ---------- Sum helpers ----------
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

# ---------- Country orchestrator (builds Final + By_Source normalized) ----------
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

    # Dedup and log
    local_sources = sorted(set(local_sources))
    int_sources   = sorted(set(int_sources))
    regional_src  = sorted(set(regional_src))
    print(f"[{country_code}] Local: {len(local_sources)}, INT: {len(int_sources)}, REG: {len(regional_src)}")

    # Numerators per domain (dataframes)
    loc_args = [(uri, d, country_name, country_code) for d in local_sources]
    int_args = [(uri, d, country_name, country_code) for d in int_sources]
    reg_args = [(uri, d, country_name, country_code) for d in regional_src]

    loc_results = p_umap(lambda a: count_domain_loc(*a), loc_args, num_cpus=num_cpus) if loc_args else []
    int_results = p_umap(lambda a: count_domain_int(*a), int_args, num_cpus=num_cpus) if int_args else []
    reg_results = p_umap(lambda a: count_domain_int(*a), reg_args, num_cpus=num_cpus) if reg_args else []

    # Map domain -> frames for each bucket
    dom2_ch, dom2_ru, dom2_cb = {}, {}, {}
    for d, (dfc, dfr, dfb) in zip(local_sources, loc_results): dom2_ch[d], dom2_ru[d], dom2_cb[d] = dfc, dfr, dfb
    for d, (dfc, dfr, dfb) in zip(int_sources, int_results):  dom2_ch[d], dom2_ru[d], dom2_cb[d] = dfc, dfr, dfb
    for d, (dfc, dfr, dfb) in zip(regional_src, reg_results): dom2_ch[d], dom2_ru[d], dom2_cb[d] = dfc, dfr, dfb

    # Denominators per domain (ALL local articles; no RAI filters)
    denom_loc = p_umap(lambda a: denom_domain_loc(*a), loc_args, num_cpus=num_cpus) if loc_args else []
    denom_int = p_umap(lambda a: denom_domain_int(*a), int_args, num_cpus=num_cpus) if int_args else []
    denom_reg = p_umap(lambda a: denom_domain_int(*a), reg_args, num_cpus=num_cpus) if reg_args else []
    country_denom = _sum_denoms(denom_loc + denom_int + denom_reg)
    denom_series = country_denom['denom_total_local'].astype('float64').mask(lambda s: s==0, np.nan)

    # Country-level raw (sum across sources)
    country_ch_raw = _sum_frames(list(dom2_ch.values()))
    country_ru_raw = _sum_frames(list(dom2_ru.values()))
    country_cb_raw = _sum_frames(list(dom2_cb.values()))

    # Add _norm columns for each event (incl '-999' to keep parity)
    for et in events:
        country_ch_raw[et + '_norm'] = country_ch_raw[et].astype('float64') / denom_series
        country_ru_raw[et + '_norm'] = country_ru_raw[et].astype('float64') / denom_series
        country_cb_raw[et + '_norm'] = country_cb_raw[et].astype('float64') / denom_series

    # Write country-level Final
    out_final = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI_New/Final_Aggregated/{country_name}/{today.year}_{today.month}_{today.day}/"
    Path(out_final).mkdir(parents=True, exist_ok=True)
    country_ch_raw.sort_index().to_csv(os.path.join(out_final, f"{country_name}_China.csv"))
    country_ru_raw.sort_index().to_csv(os.path.join(out_final, f"{country_name}_Russia.csv"))
    country_cb_raw.sort_index().to_csv(os.path.join(out_final, f"{country_name}_Combined.csv"))

    # Write by-source normalized (divide numeric cols except year/month)
    out_by_source = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_RAI_New/Normalized_By_Source/{country_name}/{today.year}_{today.month}_{today.day}/"
    for bucket in ['China','Russia','Combined']:
        Path(os.path.join(out_by_source, bucket)).mkdir(parents=True, exist_ok=True)

    def _norm_and_write(df_map, bucket):
        for domain, df in df_map.items():
            df_norm = df.copy()
            aligned = denom_series.reindex(df_norm.index)
            numeric_cols = [c for c in df_norm.columns if pd.api.types.is_numeric_dtype(df_norm[c]) and c not in ('year','month')]
            df_norm[numeric_cols] = df_norm[numeric_cols].astype('float64').div(aligned, axis=0)
            df_norm.sort_index().to_csv(os.path.join(out_by_source, bucket, f"{domain}.csv"))

    _norm_and_write(dom2_ch, 'China')
    _norm_and_write(dom2_ru, 'Russia')
    _norm_and_write(dom2_cb, 'Combined')

# ---------- Git helper (unchanged) ----------
def run_git_commands(commit_message):
    try:
        subprocess.run("git add *.py", shell=True, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

# ---------- Main ----------
if __name__ == "__main__":
    slp = False
    if slp:
        t = 7200
        print(f'Start sleeping for {t/60} mins')
        time.sleep(t)

    all_countries = [
        ('Albania', 'ALB'), ('Benin', 'BEN'), ('Colombia', 'COL'), ('Ecuador', 'ECU'),
        ('Ethiopia', 'ETH'), ('Georgia', 'GEO'), ('Kenya', 'KEN'), ('Paraguay', 'PRY'),
        ('Mali', 'MLI'), ('Morocco', 'MAR'), ('Nigeria', 'NGA'), ('Serbia', 'SRB'),
        ('Senegal', 'SEN'), ('Tanzania', 'TZA'), ('Uganda', 'UGA'), ('Ukraine', 'UKR'),
        ('Zimbabwe', 'ZWE'), ('Mauritania', 'MRT'), ('Zambia', 'ZMB'), ('Kosovo', 'XKX'),
        ('Niger', 'NER'), ('Jamaica', 'JAM'), ('Honduras', 'HND'), ('Philippines', 'PHL'),
        ('Ghana', 'GHA'), ('Rwanda', 'RWA'), ('Guatemala', 'GTM'), ('Belarus', 'BLR'),
        ('Cambodia', 'KHM'), ('DR Congo', 'COD'), ('Turkey', 'TUR'), ('Bangladesh', 'BGD'),
        ('El Salvador', 'SLV'), ('South Africa', 'ZAF'), ('Tunisia', 'TUN'), ('Indonesia', 'IDN'),
        ('Nicaragua', 'NIC'), ('Angola', 'AGO'), ('Armenia', 'ARM'), ('Sri Lanka', 'LKA'),
        ('Malaysia', 'MYS'), ('Cameroon', 'CMR'), ('Hungary', 'HUN'), ('Malawi', 'MWI'),
        ('Uzbekistan', 'UZB'), ('India', 'IND'), ('Mozambique', 'MOZ'), ('Azerbaijan', 'AZE'),
        ('Kyrgyzstan', 'KGZ'), ('Moldova', 'MDA'), ('Kazakhstan', 'KAZ'), ('Peru', 'PER'),
        ('Algeria', 'DZA'), ('Macedonia', 'MKD'), ('South Sudan', 'SSD'), ('Liberia', 'LBR'),
        ('Pakistan', 'PAK'), ('Nepal', 'NPL'), ('Namibia', 'NAM'), ('Burkina Faso', 'BFA'),
        ('Dominican Republic', 'DOM'), ('Timor Leste', 'TLS'), ('Solomon Islands', 'SLB'),
        ('Costa Rica', 'CRI'), ('Panama', 'PAN'), ('Mexico', 'MEX')
    ]

    countries_needed = [
        # 'ARM','BLR','KGZ','MKD','MDA','SRB','SLV','DOM','NIC','PRY','KHM','LKA','LBR','ZMB','ZWE','ECU','ALB','MEX','PHL','UZB','AGO','XKX','MKD','BFA','CMR'
        # 'IND','IDN','HUN','AZE','CRI','ECU','ETH','BGD','COL','DZA'
        # 'IND','IDN','HUN','AZE','CRI','ECU','ETH','BGD','COL','DZA','SRB'
        # 'ALB', 'BEN', 'COL', 'ECU', 'ETH', 'GEO', 'KEN', 'PRY', 'MLI', 'MAR', 'NGA', 'SRB', 'SEN', 'TZA', 'UGA', 'UKR', 'ZWE', 'MRT', 'ZMB', 'XKX', 'NER', 'JAM', 'HND', 'PHL', 'GHA', 'RWA', 'GTM', 'BLR', 'KHM', 'COD', 'TUR', 'BGD', 'SLV', 'ZAF', 'TUN', 'IDN', 'NIC', 'AGO', 'ARM', 'LKA', 'MYS', 'CMR', 'HUN', 'MWI', 'UZB', 'IND', 'MOZ', 'AZE', 'KGZ', 'MDA', 'KAZ', 'PER', 'DZA', 'MKD', 'SSD', 'LBR', 'PAK', 'NPL', 'NAM', 'BFA', 'DOM', 'TLS', 'SLB', 'CRI', 'PAN'
        # 'MEX'
        # 'KAZ','MWI','MRT','JAM','NAM','NGA','MYS'
        # 'KAZ','MWI','MRT','JAM','NAM','NGA','MYS','MAR','NPL','NER','PAK'
         'LBR','ZWE','ARM','ZMB','BLR','SLV'
        ]
    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

    for country_name, country_code in countries:
        print('Starting:', country_name)
        process_country(uri, country_name, country_code, num_cpus=10)
        try:
            commit_message = f"RAI counts + normalized (by-source & final) with persistent flags ({country_code})"
            # run_git_commands(commit_message)
        except Exception:
            pass

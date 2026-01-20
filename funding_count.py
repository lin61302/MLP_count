# import os
# from pathlib import Path
# import re
# import pandas as pd
# from tqdm import tqdm
# from p_tqdm import p_umap
# import time
# from dotenv import load_dotenv
# from pymongo import MongoClient
# import dateparser
# import subprocess

# # ------------------- Setup -------------------
# load_dotenv()
# URI = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
# DB = MongoClient(URI).ml4p
# TODAY = pd.Timestamp.now()
# print(TODAY)

# # ------------------- Columns: Event x Sentiment -------------------
# EVENT_LABELS = ['foreign_policy', 'foreign_aid', 'military_and_dod', '-999']
# SENT_LABELS  = ['positive', 'neutral', 'negative']
# COMBO_COLS = [f"{e}_{s}" for e in EVENT_LABELS for s in SENT_LABELS]

# # Add a total counter (optional but handy)
# ALL_COLS = COMBO_COLS + ['total_articles']

# # ------------------- Georgia text filters (unchanged) -------------------
# __georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
# __georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

# geo_int = pd.read_excel(__georgiapath_int__)
# geo_loc = pd.read_excel(__georgiapath_loc__)
# g_int = geo_int['CompanyName'].str.strip().copy()
# g_loc = geo_loc['CompanyName'].str.strip().copy()

# for i, doc in enumerate(g_int):
#     g_int[i] = "(?<![a-zA-Z])" + g_int[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"
# for i, doc in enumerate(g_loc):
#     g_loc[i] = "(?<![a-zA-Z])" + g_loc[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"

# g_int_string = '|'.join(g_int)
# g_loc_string = '|'.join(g_loc)
# g_int_filter = re.compile(g_int_string, flags=re.IGNORECASE)
# g_loc_filter = re.compile(g_loc_string, flags=re.IGNORECASE)

# def check_georgia(doc_text, domain_type):
#     """
#     Returns False if the doc contains any forbidden 'CompanyName' pattern,
#     meaning it's not truly relevant to Georgia. Original logic retained.
#     """
#     try:
#         if domain_type == 'loc':
#             return not g_loc_filter.search(doc_text)
#         else:  # 'int'
#             return not g_int_filter.search(doc_text)
#     except:
#         return True

# # ------------------- Helpers -------------------
# def _init_monthly_df():
#     df = pd.DataFrame()
#     df['date'] = pd.date_range('2012-1-1', TODAY + pd.Timedelta(31, 'd'), freq='MS')
#     df.index = df['date']
#     df['year'] = df.index.year
#     df['month'] = df.index.month
#     for col in ALL_COLS:
#         df[col] = 0
#     return df

# def _safe_month_from(doc_dt):
#     try:
#         return pd.Timestamp(doc_dt.year, doc_dt.month, 1)
#     except Exception:
#         dd = dateparser.parse(str(doc_dt)).replace(tzinfo=None)
#         return pd.Timestamp(dd.year, dd.month, 1)

# def _event_sent_key(event_label, sent_label):
#     if (event_label in EVENT_LABELS) and (sent_label in SENT_LABELS):
#         return f"{event_label}_{sent_label}"
#     return None

# # ------------------- Counting: Local -------------------
# def count_domain_loc_funding(uri, domain, country_name, country_code):
#     """
#     Count US-funding event×sentiment combos for LOCAL sources of a given country.
#     Conditions:
#       - include=True
#       - US_funding_relevance.result == 'Yes'
#       - US_funding_event + US_funding_sentiment exist
#       - language != 'en' => location via 'cliff_locations'; allow exists OR empty dict
#       - language == 'en' => location via 'en_cliff_locations'; allow exists OR empty dict
#     """
#     db = MongoClient(uri).ml4p
#     df = _init_monthly_df()
#     loc_code = country_code[-3:]

#     projection = {
#         '_id': 1, 'date_publish': 1, 'language': 1,
#         'title_translated': 1, 'maintext_translated': 1,
#         'cliff_locations': 1, 'en_cliff_locations': 1,
#         'US_funding_relevance': 1, 'US_funding_event': 1, 'US_funding_sentiment': 1
#     }

#     for date in df.index:
#         colname = f"articles-{date.year}-{date.month}"

#         # Non-English
#         cur1 = db[colname].find(
#             {
#                 'source_domain': domain,
#                 'include': True,
#                 'US_funding_relevance.result': 'Yes',
#                 'US_funding_event': {'$exists': True},
#                 'US_funding_sentiment': {'$exists': True},
#                 'language': {'$ne': 'en'},
#                 # '$or': [
#                 #     {f'cliff_locations.{loc_code}': {'$exists': True}},
#                 #     {'cliff_locations': {}}
#                 # ]
#             },
#             projection=projection,
#             batch_size=100
#         )
#         docs1 = list(cur1)

#         # English
#         cur2 = db[colname].find(
#             {
#                 'source_domain': domain,
#                 'include': True,
#                 'US_funding_relevance.result': 'Yes',
#                 'US_funding_event': {'$exists': True},
#                 'US_funding_sentiment': {'$exists': True},
#                 'language': 'en',
#                 # '$or': [
#                 #     {f'en_cliff_locations.{loc_code}': {'$exists': True}},
#                 #     {'en_cliff_locations': {}}
#                 # ]
#             },
#             projection=projection,
#             batch_size=100
#         )
#         docs2 = list(cur2)

#         docs = docs1 + docs2
#         if not docs:
#             continue

#         # Georgia text filter for local mode
#         if country_code in ('GEO', 'ENV_GEO'):
#             filtered = []
#             for d in docs:
#                 title_t = d.get('title_translated', '')
#                 main_t  = d.get('maintext_translated', '')
#                 if check_georgia(main_t, 'loc') and check_georgia(title_t, 'loc'):
#                     filtered.append(d)
#             docs = filtered

#         for d in docs:
#             doc_month = _safe_month_from(d.get('date_publish'))
#             ev  = (d.get('US_funding_event') or {}).get('result')
#             sen = (d.get('US_funding_sentiment') or {}).get('result')
#             key = _event_sent_key(ev, sen)
#             if key:
#                 df.loc[doc_month, key] += 1
#                 df.loc[doc_month, 'total_articles'] += 1

#             # Optional: mark DB for GEO like original env script did
#             if country_code in ('GEO', 'ENV_GEO'):
#                 colname_g = f"articles-{doc_month.year}-{doc_month.month}"
#                 db[colname_g].update_one({'_id': d['_id']}, {'$set': {'Country_Georgia': 'Yes'}})

#     out_path = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_USG_Funding/{country_name}/{TODAY.year}_{TODAY.month}_{TODAY.day}/"
#     Path(out_path).mkdir(parents=True, exist_ok=True)
#     df.to_csv(os.path.join(out_path, f"{domain}.csv"))

# # ------------------- Counting: International/Regional -------------------
# def count_domain_int_funding(uri, domain, country_name, country_code):
#     """
#     Count for MAJOR INTERNATIONAL + MAJOR REGIONAL sources:
#       - require cliff_locations.<code> (non-en) or en_cliff_locations.<code> (en) to exist
#       - other conditions same as local
#     """
#     db = MongoClient(uri).ml4p
#     df = _init_monthly_df()
#     loc_code = country_code[-3:]

#     projection = {
#         '_id': 1, 'date_publish': 1, 'language': 1,
#         'title_translated': 1, 'maintext_translated': 1,
#         'cliff_locations': 1, 'en_cliff_locations': 1,
#         'US_funding_relevance': 1, 'US_funding_event': 1, 'US_funding_sentiment': 1
#     }

#     for date in df.index:
#         colname = f"articles-{date.year}-{date.month}"

#         # Non-English must have location key present
#         cur1 = db[colname].find(
#             {
#                 'source_domain': domain,
#                 'include': True,
#                 'US_funding_relevance.result': 'Yes',
#                 'US_funding_event': {'$exists': True},
#                 'US_funding_sentiment': {'$exists': True},
#                 'language': {'$ne': 'en'},
#                 f'cliff_locations.{loc_code}': {'$exists': True}
#             },
#             projection=projection,
#             batch_size=100
#         )
#         docs1 = list(cur1)

#         # English must have en_cliff_locations key present
#         cur2 = db[colname].find(
#             {
#                 'source_domain': domain,
#                 'include': True,
#                 'US_funding_relevance.result': 'Yes',
#                 'US_funding_event': {'$exists': True},
#                 'US_funding_sentiment': {'$exists': True},
#                 'language': 'en',
#                 f'en_cliff_locations.{loc_code}': {'$exists': True}
#             },
#             projection=projection,
#             batch_size=100
#         )
#         docs2 = list(cur2)

#         docs = docs1 + docs2
#         if not docs:
#             continue

#         # Georgia text filter for "international" mode
#         if country_code in ('GEO', 'ENV_GEO'):
#             filtered = []
#             for d in docs:
#                 title_t = d.get('title_translated', '')
#                 main_t  = d.get('maintext_translated', '')
#                 if check_georgia(main_t, 'int') and check_georgia(title_t, 'int'):
#                     filtered.append(d)
#             docs = filtered

#         for d in docs:
#             doc_month = _safe_month_from(d.get('date_publish'))
#             ev  = (d.get('US_funding_event') or {}).get('result')
#             sen = (d.get('US_funding_sentiment') or {}).get('result')
#             key = _event_sent_key(ev, sen)
#             if key:
#                 df.loc[doc_month, key] += 1
#                 df.loc[doc_month, 'total_articles'] += 1

#             if country_code in ('GEO', 'ENV_GEO'):
#                 colname_g = f"articles-{doc_month.year}-{doc_month.month}"
#                 db[colname_g].update_one({'_id': d['_id']}, {'$set': {'Country_Georgia': 'Yes'}})

#     out_path = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_USG_Funding/{country_name}/{TODAY.year}_{TODAY.month}_{TODAY.day}/"
#     Path(out_path).mkdir(parents=True, exist_ok=True)
#     df.to_csv(os.path.join(out_path, f"{domain}.csv"))

# # ------------------- Git helper -------------------
# def run_git_commands(commit_message):
#     try:
#         subprocess.run("git add *.py", shell=True, check=True)
#         subprocess.run(["git", "commit", "-m", commit_message], check=True)
#         subprocess.run(["git", "push"], check=True)
#         print("Git commands executed successfully!")
#     except subprocess.CalledProcessError as e:
#         print(f"An error occurred while running Git commands: {e}")

# # ------------------- Main -------------------
# if __name__ == "__main__":
#     slp = False
#     if slp:
#         t = 7200
#         print(f'start sleeping for {t/60} mins')
#         time.sleep(t)

#     # Example driver (same style as your env script):
#     countries_needed = [
#         # put ISO3 codes you want to run, e.g. 'PAN','CRI'
#         # 'MEX','LBR','MDA','SRB','LKA','KGZ','PHL'
#         #  'MOZ','COD','SSD','ZWE','GHA','KHM'
#         # 'BEN', 'UKR', 'GEO', 'GTM','NIC', 'PRY'
#         # 'MEX','LBR','MDA','SRB','LKA','KGZ','PHL'
#         # 'ARM','BLR','KGZ','MKD','MDA','SRB','SLV','DOM','NIC','PRY','KHM','LKA','LBR','ZMB','ZWE','ECU','ALB','MEX','PHL','UZB','AGO','XKX','MKD','BFA','CMR'
#         # 'AZE','GEO','HUN','UKR'
#         # 'IND','IDN','HUN','AZE','CRI','ECU','ETH','BGD','COL','DZA','SRB'
#         # 'KAZ','MWI','MRT','JAM','NAM','NGA','MYS'
#         # 'KAZ','MWI','MRT','JAM','NAM','NGA','MYS','MAR','NPL','NER','PAK','MEX
#          'LBR','ZWE','ARM','ZMB','BLR','SLV'
        
#     ]
#     all_countries = [
#         ('Albania', 'ALB'), ('Benin', 'BEN'), ('Colombia', 'COL'), ('Ecuador', 'ECU'),
#         ('Ethiopia', 'ETH'), ('Georgia', 'GEO'), ('Kenya', 'KEN'), ('Paraguay', 'PRY'),
#         ('Mali', 'MLI'), ('Morocco', 'MAR'), ('Nigeria', 'NGA'), ('Serbia', 'SRB'),
#         ('Senegal', 'SEN'), ('Tanzania', 'TZA'), ('Uganda', 'UGA'), ('Ukraine', 'UKR'),
#         ('Zimbabwe', 'ZWE'), ('Mauritania', 'MRT'), ('Zambia', 'ZMB'), ('Kosovo', 'XKX'),
#         ('Niger', 'NER'), ('Jamaica', 'JAM'), ('Honduras', 'HND'), ('Philippines', 'PHL'),
#         ('Ghana', 'GHA'), ('Rwanda','RWA'), ('Guatemala','GTM'), ('Belarus','BLR'),
#         ('Cambodia','KHM'), ('DR Congo','COD'), ('Turkey','TUR'), ('Bangladesh','BGD'),
#         ('El Salvador','SLV'), ('South Africa','ZAF'), ('Tunisia','TUN'), ('Indonesia','IDN'),
#         ('Nicaragua','NIC'), ('Angola','AGO'), ('Armenia','ARM'), ('Sri Lanka','LKA'),
#         ('Malaysia','MYS'), ('Cameroon','CMR'), ('Hungary','HUN'), ('Malawi','MWI'),
#         ('Uzbekistan','UZB'), ('India','IND'), ('Mozambique','MOZ'), ('Azerbaijan','AZE'),
#         ('Kyrgyzstan','KGZ'), ('Moldova','MDA'), ('Kazakhstan','KAZ'), ('Peru','PER'),
#         ('Algeria','DZA'), ('Macedonia','MKD'), ('South Sudan','SSD'), ('Liberia','LBR'),
#         ('Pakistan','PAK'), ('Nepal','NPL'), ('Namibia','NAM'), ('Burkina Faso','BFA'),
#         ('Dominican Republic','DOM'), ('Timor Leste','TLS'), ('Solomon Islands','SLB'),
#         ("Costa Rica",'CRI'), ('Panama','PAN'),('Mexico','MEX')
#     ]

#     countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

#     for (country_name, country_code) in countries:
#         print('Starting:', country_name)

#         db_mongo = MongoClient(URI).ml4p

#         # Local sources (primary_location == country_code)
#         loc = [doc['source_domain'] for doc in db_mongo['sources'].find(
#             {'primary_location': {'$in': [country_code]}, 'include': True}
#         )]

#         # Major international + regional (no ENV_ handling here)
#         ints = [doc['source_domain'] for doc in db_mongo['sources'].find(
#             {'major_international': True, 'include': True}
#         )]
#         regionals = [doc['source_domain'] for doc in db_mongo['sources'].find(
#             {'major_regional': True, 'include': True}
#         )]
#         mlp_int = list(set(ints + regionals))

#         # Parallel counts
#         if loc:
#             p_umap(count_domain_loc_funding, [URI]*len(loc), loc,
#                    [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)

#         if mlp_int:
#             p_umap(count_domain_int_funding, [URI]*len(mlp_int), mlp_int,
#                    [country_name]*len(mlp_int), [country_code]*len(mlp_int), num_cpus=10)

#         commit_message = f"US_funding classifier count ({country_code}) update"
#         run_git_commands(commit_message)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
USG Funding counters with civic-style folder structure (no civic/ncr/combined layers).

Outputs:
- Raw by source:
  Counts_USG_Funding/Raw_By_Source/{country}/{YYYY_M_D}/{domain}.csv
- Normalized by source (divide each numeric col except year/month by SAME country-month denominator):
  Counts_USG_Funding/Normalized_By_Source/{country}/{YYYY_M_D}/{domain}.csv
- Final aggregated (one file per country, sum across sources; includes *_norm columns):
  Counts_USG_Funding/Final_Aggregated/{country}/{YYYY_M_D}/{country}_USG_Funding.csv

Normalization denominator per month = ALL articles considered "local information environment"
across ALL sources, using the SAME local vs int/reg source grouping pattern as Civic:

- Local sources: include=True, language split only (no location constraint),
  matching the current funding local counting logic (your cliff filter is commented out).
- Major international + major regional sources: include=True AND location key exists
  (cliff_locations.<ISO3> for non-en, en_cliff_locations.<ISO3> for en),
  matching the current funding int/reg counting logic.
- Georgia false-positive filters applied to BOTH numerators and denominators.
"""

import os
from pathlib import Path
import re
import numpy as np
import pandas as pd
from p_tqdm import p_umap
import time
from dotenv import load_dotenv
from pymongo import MongoClient
import dateparser
import subprocess

# ------------------- Setup -------------------
load_dotenv()
URI = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
DB = MongoClient(URI).ml4p
TODAY = pd.Timestamp.now()

# Root output directory (matches your Dropbox pattern)
OUT_ROOT = "/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_USG_Funding"
DATE_STAMP = f"{TODAY.year}_{TODAY.month}_{TODAY.day}"

print("Run date:", TODAY)

# ------------------- Columns: Event x Sentiment -------------------
EVENT_LABELS = ['foreign_policy', 'foreign_aid', 'military_and_dod', '-999']
SENT_LABELS  = ['positive', 'neutral', 'negative']
COMBO_COLS = [f"{e}_{s}" for e in EVENT_LABELS for s in SENT_LABELS]
ALL_COLS = COMBO_COLS + ['total_articles']  # keep as in your script

# ------------------- Georgia text filters (UNCHANGED) -------------------
__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip().copy()
g_loc = geo_loc['CompanyName'].str.strip().copy()

for i, doc in enumerate(g_int):
    g_int[i] = "(?<![a-zA-Z])" + g_int[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"
for i, doc in enumerate(g_loc):
    g_loc[i] = "(?<![a-zA-Z])" + g_loc[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"

g_int_string = '|'.join(g_int)
g_loc_string = '|'.join(g_loc)
g_int_filter = re.compile(g_int_string, flags=re.IGNORECASE)
g_loc_filter = re.compile(g_loc_string, flags=re.IGNORECASE)

def check_georgia(doc_text, domain_type):
    """
    Returns False if the doc contains any forbidden 'CompanyName' pattern,
    meaning it's not truly relevant to Georgia. Original logic retained.
    """
    try:
        if domain_type == 'loc':
            return not g_loc_filter.search(doc_text)
        else:  # 'int'
            return not g_int_filter.search(doc_text)
    except:
        return True

# ------------------- Helpers -------------------
def _init_monthly_df():
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', TODAY + pd.Timedelta(31, 'd'), freq='MS')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month
    for col in ALL_COLS:
        df[col] = 0
    return df

def _init_monthly_denom_df():
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', TODAY + pd.Timedelta(31, 'd'), freq='MS')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month
    df['denom_total_local'] = 0
    return df

def _safe_month_from(doc_dt):
    try:
        return pd.Timestamp(doc_dt.year, doc_dt.month, 1)
    except Exception:
        dd = dateparser.parse(str(doc_dt)).replace(tzinfo=None)
        return pd.Timestamp(dd.year, dd.month, 1)

def _event_sent_key(event_label, sent_label):
    if (event_label in EVENT_LABELS) and (sent_label in SENT_LABELS):
        return f"{event_label}_{sent_label}"
    return None

def _sum_frames(frames):
    """
    Sum per-domain monthly frames into a country-level monthly frame.
    Resets year/month/date after summing (like civic).
    """
    if not frames:
        return _init_monthly_df()

    out = pd.concat(frames).groupby(level=0).sum(numeric_only=True)

    # Restore date/year/month columns from index (avoid summed year/month)
    out['date'] = out.index
    out['year'] = out.index.year
    out['month'] = out.index.month

    # Ensure all expected columns exist (in case of weird empties)
    for c in ALL_COLS:
        if c not in out.columns:
            out[c] = 0

    # Order columns consistently
    out = out[['date', 'year', 'month'] + ALL_COLS]
    return out

def _sum_denoms(frames):
    if not frames:
        return _init_monthly_denom_df()

    out = pd.concat(frames).groupby(level=0).sum(numeric_only=True)
    out['date'] = out.index
    out['year'] = out.index.year
    out['month'] = out.index.month

    if 'denom_total_local' not in out.columns:
        out['denom_total_local'] = 0

    out = out[['date', 'year', 'month', 'denom_total_local']]
    return out

# ------------------- Counting: Local (UNCHANGED counting logic) -------------------
def count_domain_loc_funding(uri, domain, country_name, country_code):
    """
    Count US-funding event×sentiment combos for LOCAL sources of a given country.
    Conditions:
      - include=True
      - US_funding_relevance.result == 'Yes'
      - US_funding_event + US_funding_sentiment exist
      - language != 'en' => query as non-en
      - language == 'en' => query as en
    (Location constraints remain commented out, exactly as in your script.)
    """
    db = MongoClient(uri).ml4p
    df = _init_monthly_df()
    loc_code = country_code[-3:]

    projection = {
        '_id': 1, 'date_publish': 1, 'language': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'cliff_locations': 1, 'en_cliff_locations': 1,
        'US_funding_relevance': 1, 'US_funding_event': 1, 'US_funding_sentiment': 1
    }

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        # Non-English
        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'US_funding_relevance.result': 'Yes',
                'US_funding_event': {'$exists': True},
                'US_funding_sentiment': {'$exists': True},
                'language': {'$ne': 'en'},
                # '$or': [
                #     {f'cliff_locations.{loc_code}': {'$exists': True}},
                #     {'cliff_locations': {}}
                # ]
            },
            projection=projection,
            batch_size=100
        )
        docs1 = list(cur1)

        # English
        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'US_funding_relevance.result': 'Yes',
                'US_funding_event': {'$exists': True},
                'US_funding_sentiment': {'$exists': True},
                'language': 'en',
                # '$or': [
                #     {f'en_cliff_locations.{loc_code}': {'$exists': True}},
                #     {'en_cliff_locations': {}}
                # ]
            },
            projection=projection,
            batch_size=100
        )
        docs2 = list(cur2)

        docs = docs1 + docs2
        if not docs:
            continue

        # Georgia text filter for local mode
        if country_code in ('GEO', 'ENV_GEO'):
            filtered = []
            for d in docs:
                title_t = d.get('title_translated', '')
                main_t  = d.get('maintext_translated', '')
                if check_georgia(main_t, 'loc') and check_georgia(title_t, 'loc'):
                    filtered.append(d)
            docs = filtered

        for d in docs:
            doc_month = _safe_month_from(d.get('date_publish'))
            ev  = (d.get('US_funding_event') or {}).get('result')
            sen = (d.get('US_funding_sentiment') or {}).get('result')
            key = _event_sent_key(ev, sen)
            if key:
                df.loc[doc_month, key] += 1
                df.loc[doc_month, 'total_articles'] += 1

            # Optional: mark DB for GEO like original behavior
            if country_code in ('GEO', 'ENV_GEO'):
                colname_g = f"articles-{doc_month.year}-{doc_month.month}"
                db[colname_g].update_one({'_id': d['_id']}, {'$set': {'Country_Georgia': 'Yes'}})

    # Write RAW by source (new folder structure)
    out_path = os.path.join(OUT_ROOT, "Raw_By_Source", country_name, DATE_STAMP)
    Path(out_path).mkdir(parents=True, exist_ok=True)
    df.sort_index().to_csv(os.path.join(out_path, f"{domain}.csv"))

    return df

# ------------------- Counting: International/Regional (UNCHANGED counting logic) -------------------
def count_domain_int_funding(uri, domain, country_name, country_code):
    """
    Count for MAJOR INTERNATIONAL + MAJOR REGIONAL sources:
      - require cliff_locations.<code> (non-en) or en_cliff_locations.<code> (en) to exist
      - other conditions same as local
    """
    db = MongoClient(uri).ml4p
    df = _init_monthly_df()
    loc_code = country_code[-3:]

    projection = {
        '_id': 1, 'date_publish': 1, 'language': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'cliff_locations': 1, 'en_cliff_locations': 1,
        'US_funding_relevance': 1, 'US_funding_event': 1, 'US_funding_sentiment': 1
    }

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        # Non-English must have location key present
        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'US_funding_relevance.result': 'Yes',
                'US_funding_event': {'$exists': True},
                'US_funding_sentiment': {'$exists': True},
                'language': {'$ne': 'en'},
                f'cliff_locations.{loc_code}': {'$exists': True}
            },
            projection=projection,
            batch_size=100
        )
        docs1 = list(cur1)

        # English must have en_cliff_locations key present
        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'US_funding_relevance.result': 'Yes',
                'US_funding_event': {'$exists': True},
                'US_funding_sentiment': {'$exists': True},
                'language': 'en',
                f'en_cliff_locations.{loc_code}': {'$exists': True}
            },
            projection=projection,
            batch_size=100
        )
        docs2 = list(cur2)

        docs = docs1 + docs2
        if not docs:
            continue

        # Georgia text filter for "international" mode
        if country_code in ('GEO', 'ENV_GEO'):
            filtered = []
            for d in docs:
                title_t = d.get('title_translated', '')
                main_t  = d.get('maintext_translated', '')
                if check_georgia(main_t, 'int') and check_georgia(title_t, 'int'):
                    filtered.append(d)
            docs = filtered

        for d in docs:
            doc_month = _safe_month_from(d.get('date_publish'))
            ev  = (d.get('US_funding_event') or {}).get('result')
            sen = (d.get('US_funding_sentiment') or {}).get('result')
            key = _event_sent_key(ev, sen)
            if key:
                df.loc[doc_month, key] += 1
                df.loc[doc_month, 'total_articles'] += 1

            if country_code in ('GEO', 'ENV_GEO'):
                colname_g = f"articles-{doc_month.year}-{doc_month.month}"
                db[colname_g].update_one({'_id': d['_id']}, {'$set': {'Country_Georgia': 'Yes'}})

    # Write RAW by source (new folder structure)
    out_path = os.path.join(OUT_ROOT, "Raw_By_Source", country_name, DATE_STAMP)
    Path(out_path).mkdir(parents=True, exist_ok=True)
    df.sort_index().to_csv(os.path.join(out_path, f"{domain}.csv"))

    return df

# ------------------- Denominators: ALL local articles (for normalization) -------------------
def denom_domain_loc_all(uri, domain, country_name, country_code):
    """
    Denominator for LOCAL sources:
      - counts ALL include=True articles in that source, split by language,
        with NO location constraints (mirrors current funding local logic).
    """
    db = MongoClient(uri).ml4p
    df = _init_monthly_denom_df()

    # Only need these fields for Georgia filtering
    proj_geo = {'title_translated': 1, 'maintext_translated': 1, '_id': 0, 'language': 1}

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        q_non_en = {'source_domain': domain, 'include': True, 'language': {'$ne': 'en'}}
        q_en     = {'source_domain': domain, 'include': True, 'language': 'en'}

        if country_code in ('GEO', 'ENV_GEO'):
            docs = list(db[colname].find(q_non_en, projection=proj_geo, batch_size=200)) + \
                   list(db[colname].find(q_en,     projection=proj_geo, batch_size=200))
            cnt = 0
            for d in docs:
                t = d.get('title_translated', '') or ''
                m = d.get('maintext_translated', '') or ''
                if check_georgia(m, 'loc') and check_georgia(t, 'loc'):
                    cnt += 1
        else:
            cnt = db[colname].count_documents(q_non_en) + db[colname].count_documents(q_en)

        df.loc[date, 'denom_total_local'] = cnt

    return df

def denom_domain_int_all(uri, domain, country_name, country_code):
    """
    Denominator for INTERNATIONAL/REGIONAL sources:
      - counts ALL include=True articles that have a country location key (strict),
        split by language (mirrors current funding int/reg logic).
    """
    db = MongoClient(uri).ml4p
    df = _init_monthly_denom_df()
    loc_code = country_code[-3:]

    proj_geo = {'title_translated': 1, 'maintext_translated': 1, '_id': 0, 'language': 1}

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        q_non_en = {
            'source_domain': domain, 'include': True, 'language': {'$ne': 'en'},
            f'cliff_locations.{loc_code}': {'$exists': True}
        }
        q_en = {
            'source_domain': domain, 'include': True, 'language': 'en',
            f'en_cliff_locations.{loc_code}': {'$exists': True}
        }

        if country_code in ('GEO', 'ENV_GEO'):
            docs = list(db[colname].find(q_non_en, projection=proj_geo, batch_size=200)) + \
                   list(db[colname].find(q_en,     projection=proj_geo, batch_size=200))
            cnt = 0
            for d in docs:
                t = d.get('title_translated', '') or ''
                m = d.get('maintext_translated', '') or ''
                if check_georgia(m, 'int') and check_georgia(t, 'int'):
                    cnt += 1
        else:
            cnt = db[colname].count_documents(q_non_en) + db[colname].count_documents(q_en)

        df.loc[date, 'denom_total_local'] = cnt

    return df

# ------------------- Country orchestrator -------------------
def process_country(uri, country_name, country_code, num_cpus=10):
    dbm = MongoClient(uri).ml4p

    # Source sets (same basic grouping as your funding script)
    local_sources = [d['source_domain'] for d in dbm['sources'].find(
        {'primary_location': {'$in': [country_code]}, 'include': True}
    )]

    int_sources = [d['source_domain'] for d in dbm['sources'].find(
        {'major_international': True, 'include': True}
    )]
    regional_sources = [d['source_domain'] for d in dbm['sources'].find(
        {'major_regional': True, 'include': True}
    )]

    # De-dup
    local_sources = sorted(set(local_sources))
    int_sources = sorted(set(int_sources))
    regional_sources = sorted(set(regional_sources))

    # (Optional) avoid overlap between sets to prevent double counting
    int_sources = [d for d in int_sources if d not in local_sources]
    regional_sources = [d for d in regional_sources if (d not in local_sources and d not in int_sources)]

    print(f"[{country_code}] Local={len(local_sources)} | INT={len(int_sources)} | REG={len(regional_sources)}")

    # ---- Run per-domain funding counters (RAW) ----
    loc_args = [(uri, d, country_name, country_code) for d in local_sources]
    int_args = [(uri, d, country_name, country_code) for d in int_sources]
    reg_args = [(uri, d, country_name, country_code) for d in regional_sources]

    loc_results = p_umap(lambda a: count_domain_loc_funding(*a), loc_args, num_cpus=num_cpus) if loc_args else []
    int_results = p_umap(lambda a: count_domain_int_funding(*a), int_args, num_cpus=num_cpus) if int_args else []
    reg_results = p_umap(lambda a: count_domain_int_funding(*a), reg_args, num_cpus=num_cpus) if reg_args else []

    # Map domain -> df
    domain_to_df = {}
    for d, df in zip(local_sources, loc_results):
        domain_to_df[d] = df
    for d, df in zip(int_sources, int_results):
        domain_to_df[d] = df
    for d, df in zip(regional_sources, reg_results):
        domain_to_df[d] = df

    # ---- Denominators per domain (ALL local articles, no funding filters) ----
    denom_loc = p_umap(lambda a: denom_domain_loc_all(*a), loc_args, num_cpus=num_cpus) if loc_args else []
    denom_int = p_umap(lambda a: denom_domain_int_all(*a), int_args, num_cpus=num_cpus) if int_args else []
    denom_reg = p_umap(lambda a: denom_domain_int_all(*a), reg_args, num_cpus=num_cpus) if reg_args else []

    country_denom_df = _sum_denoms(denom_loc + denom_int + denom_reg)
    denom_series = country_denom_df['denom_total_local'].astype('float64')
    denom_series = denom_series.mask(denom_series == 0, np.nan)

    # ---- Final aggregated (sum across sources) ----
    country_raw = _sum_frames(list(domain_to_df.values()))

    # Add *_norm columns (divide by country-month denom)
    aligned = denom_series.reindex(country_raw.index)
    for c in ALL_COLS:
        country_raw[c + "_norm"] = country_raw[c].astype('float64') / aligned

    out_final = os.path.join(OUT_ROOT, "Final_Aggregated", country_name, DATE_STAMP)
    Path(out_final).mkdir(parents=True, exist_ok=True)
    country_raw.sort_index().to_csv(os.path.join(out_final, f"{country_name}_USG_Funding.csv"))

    # ---- Normalized by source (divide each numeric col except year/month by SAME country denom) ----
    out_norm = os.path.join(OUT_ROOT, "Normalized_By_Source", country_name, DATE_STAMP)
    Path(out_norm).mkdir(parents=True, exist_ok=True)

    for domain, df in domain_to_df.items():
        df_norm = df.copy()
        aligned = denom_series.reindex(df_norm.index)

        numeric_cols = [
            c for c in df_norm.columns
            if pd.api.types.is_numeric_dtype(df_norm[c]) and c not in ('year', 'month')
        ]
        df_norm[numeric_cols] = df_norm[numeric_cols].astype('float64').div(aligned, axis=0)

        df_norm.sort_index().to_csv(os.path.join(out_norm, f"{domain}.csv"))

    print(f"[{country_code}] Wrote Raw_By_Source, Normalized_By_Source, Final_Aggregated")

# ------------------- Git helper -------------------
def run_git_commands(commit_message):
    try:
        subprocess.run("git add *.py", shell=True, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

# ------------------- Main -------------------
if __name__ == "__main__":
    slp = False
    if slp:
        t = 7200
        print(f"start sleeping for {t/60} mins")
        time.sleep(t)

    countries_needed = [
        # 'ALB', 'BEN', 'COL', 'ECU', 'ETH', 'GEO', 'KEN', 'PRY', 'MLI', 'MAR', 'NGA', 'SRB', 'SEN', 'TZA', 'UGA',
        #     'UKR', 'ZWE', 'MRT', 'ZMB', 'XKX', 'NER', 'JAM', 'HND', 'PHL', 'GHA', 'RWA', 'GTM', 'BLR', 'KHM', 'COD',
        #     'TUR', 'BGD', 'SLV', 'ZAF', 'TUN', 'IDN', 'NIC', 'AGO', 'ARM', 'LKA', 'MYS', 'CMR', 'HUN', 'MWI', 'UZB',
        #     'IND', 'MOZ', 'AZE', 'KGZ', 'MDA', 'KAZ', 'PER', 'DZA', 'MKD', 'SSD', 'LBR', 'PAK', 'NPL', 'NAM', 'BFA',
        #     'DOM', 'TLS', 'SLB', 'CRI', 'PAN','MEX'
        'PRY','DOM','ECU','LKA','SRB','NIC','KHM','MDA'
    ]

    all_countries = [
        ('Albania', 'ALB'), ('Benin', 'BEN'), ('Colombia', 'COL'), ('Ecuador', 'ECU'),
        ('Ethiopia', 'ETH'), ('Georgia', 'GEO'), ('Kenya', 'KEN'), ('Paraguay', 'PRY'),
        ('Mali', 'MLI'), ('Morocco', 'MAR'), ('Nigeria', 'NGA'), ('Serbia', 'SRB'),
        ('Senegal', 'SEN'), ('Tanzania', 'TZA'), ('Uganda', 'UGA'), ('Ukraine', 'UKR'),
        ('Zimbabwe', 'ZWE'), ('Mauritania', 'MRT'), ('Zambia', 'ZMB'), ('Kosovo', 'XKX'),
        ('Niger', 'NER'), ('Jamaica', 'JAM'), ('Honduras', 'HND'), ('Philippines', 'PHL'),
        ('Ghana', 'GHA'), ('Rwanda','RWA'), ('Guatemala','GTM'), ('Belarus','BLR'),
        ('Cambodia','KHM'), ('DR Congo','COD'), ('Turkey','TUR'), ('Bangladesh','BGD'),
        ('El Salvador','SLV'), ('South Africa','ZAF'), ('Tunisia','TUN'), ('Indonesia','IDN'),
        ('Nicaragua','NIC'), ('Angola','AGO'), ('Armenia','ARM'), ('Sri Lanka','LKA'),
        ('Malaysia','MYS'), ('Cameroon','CMR'), ('Hungary','HUN'), ('Malawi','MWI'),
        ('Uzbekistan','UZB'), ('India','IND'), ('Mozambique','MOZ'), ('Azerbaijan','AZE'),
        ('Kyrgyzstan','KGZ'), ('Moldova','MDA'), ('Kazakhstan','KAZ'), ('Peru','PER'),
        ('Algeria','DZA'), ('Macedonia','MKD'), ('South Sudan','SSD'), ('Liberia','LBR'),
        ('Pakistan','PAK'), ('Nepal','NPL'), ('Namibia','NAM'), ('Burkina Faso','BFA'),
        ('Dominican Republic','DOM'), ('Timor Leste','TLS'), ('Solomon Islands','SLB'),
        ("Costa Rica",'CRI'), ('Panama','PAN'),('Mexico','MEX')
    ]

    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

    for (country_name, country_code) in countries:
        print('Starting:', country_name)
        process_country(URI, country_name, country_code, num_cpus=10)

        commit_message = f"USG funding counts (raw/by-source norm/final) ({country_code})"
        run_git_commands(commit_message)

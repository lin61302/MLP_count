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
import subprocess

# -----------------------------
# Setup
# -----------------------------
load_dotenv()
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p
today = pd.Timestamp.now()
print(today)

# Pull env labels once; create category list (exclude helper column)
env_labels = list(
    db.models.find_one({'model_name': 'env_classifier'}).get('event_type_nums').keys()
)
env_labels.append('total_articles')
category_labels = [l for l in env_labels if l != 'total_articles']

# -----------------------------
# Georgia text filters (unchanged)
# -----------------------------
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

def check_georgia(doc_text: str, domain_type: str) -> bool:
    """False if doc contains forbidden 'CompanyName' patterns."""
    try:
        if domain_type == 'loc':
            return not g_loc_filter.search(doc_text or "")
        else:  # 'int'
            return not g_int_filter.search(doc_text or "")
    except Exception:
        return True

# -----------------------------
# Helpers
# -----------------------------
def monthly_frame(start='2012-01-01'):
    df = pd.DataFrame({'date': pd.date_range(start, today + pd.Timedelta(31, 'd'), freq='MS')})
    df = df.set_index('date')
    df['year'] = df.index.year
    df['month'] = df.index.month
    # initialize counters
    for label in env_labels:
        df[label] = 0
    df['total_from_source'] = 0          # environmental docs per source/month (as before)
    df['total_label_events'] = 0          # env_max + env_sec
    df['total_local_docs'] = 0            # NEW: all local-considered docs per source/month (denominator contributor)
    return df

def sum_dfs(dfs):
    """Sum a list of monthly dataframes with the same index."""
    if not dfs:
        return monthly_frame()
    out = pd.concat(dfs).groupby(level=0).sum(numeric_only=True)
    # restore year/month from index (avoid summed years/months)
    out['year'] = out.index.year
    out['month'] = out.index.month
    return out

# common projection for Mongo queries
projection_common = {
    '_id': 1, 'env_classifier': 1,
    'title_translated': 1, 'maintext_translated': 1,
    'cliff_locations': 1, 'en_cliff_locations': 1,
    'language': 1, 'include': 1, 'source_domain': 1
}

# -----------------------------
# Per-domain counting functions
# -----------------------------
def _apply_georgia_docs(docs, mode):
    """Filter docs list with Georgia rules for 'loc' or 'int'."""
    filtered = []
    for d in docs:
        title_t = d.get('title_translated', '')
        main_t  = d.get('maintext_translated', '')
        if check_georgia(main_t, mode) and check_georgia(title_t, mode):
            filtered.append(d)
    return filtered

def count_domain_loc_env(args):
    """
    Count for a single 'local-like' domain:
    - local ABC and local ENV_ABC sources
    - location filter: allow present OR empty OR missing (lenient)
    """
    (uri_local, domain, country_name, country_code) = args
    db_local = MongoClient(uri_local).ml4p
    loc_code = country_code[-3:]

    df = monthly_frame()

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        # --------- Denominator queries (ALL local-considered docs; no env filters) ----------
        q_all_non_en = {
            'source_domain': domain,
            'include': True,
            'language': {'$ne': 'en'},
            '$or': [
                {f'cliff_locations.{loc_code}': {'$exists': True}},
                {'cliff_locations': {}},
                {'cliff_locations': {'$exists': False}}
            ]
        }
        q_all_en = {
            'source_domain': domain,
            'include': True,
            'language': 'en',
            '$or': [
                {f'en_cliff_locations.{loc_code}': {'$exists': True}},
                {'en_cliff_locations': {}},
                {'en_cliff_locations': {'$exists': False}}
            ]
        }

        # Fast path: count_documents unless Georgia (needs text)
        if country_code in ('GEO', 'ENV_GEO'):
            docs_all1 = list(db_local[colname].find(q_all_non_en, projection=projection_common, batch_size=100))
            docs_all2 = list(db_local[colname].find(q_all_en,     projection=projection_common, batch_size=100))
            docs_all  = _apply_georgia_docs(docs_all1 + docs_all2, 'loc')
            denom_count = len(docs_all)
        else:
            denom_count = db_local[colname].count_documents(q_all_non_en) + db_local[colname].count_documents(q_all_en)

        # --------- Numerator pool (ENV docs only; same as before) ----------
        q_env_non_en = {
            **q_all_non_en,
            'environmental_binary.result': 'Yes',
            'env_classifier': {'$exists': True},
        }
        q_env_en = {
            **q_all_en,
            'environmental_binary.result': 'Yes',
            'env_classifier': {'$exists': True},
        }

        docs1 = list(db_local[colname].find(q_env_non_en, projection=projection_common, batch_size=100))
        docs2 = list(db_local[colname].find(q_env_en,     projection=projection_common, batch_size=100))
        docs  = docs1 + docs2

        # Georgia filter for numerator too
        if country_code in ('GEO', 'ENV_GEO'):
            docs = _apply_georgia_docs(docs, 'loc')

        # Record denominators
        df.loc[date, 'total_local_docs'] = denom_count          # NEW denominator contributor
        df.loc[date, 'total_from_source'] = len(docs)           # environmental docs (as before)

        if not docs:
            continue

        doc_date = pd.Timestamp(date.year, date.month, 1)
        label_events_this_month = 0
        counted_docs_this_month = 0

        for d in docs:
            env_info = d.get('env_classifier') or {}
            env_max = env_info.get('env_max')
            env_sec = env_info.get('env_sec')

            if env_max in category_labels:
                df.loc[doc_date, env_max] += 1
                label_events_this_month += 1
            if env_sec in category_labels:
                df.loc[doc_date, env_sec] += 1
                label_events_this_month += 1

            counted_docs_this_month += 1

            if country_code in ('GEO', 'ENV_GEO'):
                colname_g = f"articles-{doc_date.year}-{doc_date.month}"
                db_local[colname_g].update_one({'_id': d['_id']}, {'$set': {'Country_Georgia': 'Yes'}})

        df.loc[doc_date, 'total_articles']     += counted_docs_this_month
        df.loc[doc_date, 'total_label_events'] += label_events_this_month

    return df


def count_domain_int_env(args):
    """
    Count for a single 'international/regional' domain:
    - strict location filter: require mention present for ABC
    """
    (uri_local, domain, country_name, country_code) = args
    db_local = MongoClient(uri_local).ml4p
    loc_code = country_code[-3:]

    df = monthly_frame()

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        # --------- Denominator queries (ALL local-considered docs; no env filters) ----------
        q_all_non_en = {
            'source_domain': domain,
            'include': True,
            'language': {'$ne': 'en'},
            f'cliff_locations.{loc_code}': {'$exists': True}
        }
        q_all_en = {
            'source_domain': domain,
            'include': True,
            'language': 'en',
            f'en_cliff_locations.{loc_code}': {'$exists': True}
        }

        if country_code in ('GEO', 'ENV_GEO'):
            docs_all1 = list(db_local[colname].find(q_all_non_en, projection=projection_common, batch_size=100))
            docs_all2 = list(db_local[colname].find(q_all_en,     projection=projection_common, batch_size=100))
            docs_all  = _apply_georgia_docs(docs_all1 + docs_all2, 'int')
            denom_count = len(docs_all)
        else:
            denom_count = db_local[colname].count_documents(q_all_non_en) + db_local[colname].count_documents(q_all_en)

        # --------- Numerator pool (ENV docs only; same as before) ----------
        q_env_non_en = {
            **q_all_non_en,
            'environmental_binary.result': 'Yes',
            'env_classifier': {'$exists': True},
        }
        q_env_en = {
            **q_all_en,
            'environmental_binary.result': 'Yes',
            'env_classifier': {'$exists': True},
        }

        docs1 = list(db_local[colname].find(q_env_non_en, projection=projection_common, batch_size=100))
        docs2 = list(db_local[colname].find(q_env_en,     projection=projection_common, batch_size=100))
        docs  = docs1 + docs2

        if country_code in ('GEO', 'ENV_GEO'):
            docs = _apply_georgia_docs(docs, 'int')

        # Record denominators
        df.loc[date, 'total_local_docs'] = denom_count
        df.loc[date, 'total_from_source'] = len(docs)

        if not docs:
            continue

        doc_date = pd.Timestamp(date.year, date.month, 1)
        label_events_this_month = 0
        counted_docs_this_month = 0

        for d in docs:
            env_info = d.get('env_classifier') or {}
            env_max = env_info.get('env_max')
            env_sec = env_info.get('env_sec')

            if env_max in category_labels:
                df.loc[doc_date, env_max] += 1
                label_events_this_month += 1
            if env_sec in category_labels:
                df.loc[doc_date, env_sec] += 1
                label_events_this_month += 1

            counted_docs_this_month += 1

            if country_code in ('GEO', 'ENV_GEO'):
                colname_g = f"articles-{doc_date.year}-{doc_date.month}"
                db_local[colname_g].update_one({'_id': d['_id']}, {'$set': {'Country_Georgia': 'Yes'}})

        df.loc[doc_date, 'total_articles']     += counted_docs_this_month
        df.loc[doc_date, 'total_label_events'] += label_events_this_month

    return df

# -----------------------------
# Orchestrator for a country
# -----------------------------
def process_country(uri, country_name, country_code, num_cpus=10):
    """
    For one base country code (e.g., 'IND'):
      - Pull local (ABC), env-local (ENV_ABC), int+regional sources
      - Count per domain in parallel
      - Aggregate raw across all sources
      - Normalize per month by TOTAL LOCAL DOCS across all sources (new requirement)
      - Write:
          (A) Country-level (raw + _norm) -> Counts_Env_Norm/Final/{country}/{date}/{country}.csv
          (B) Source-level normalized-only -> Counts_Env_Norm/By_Source/{country}/{date}/{domain}.csv
    """
    db_mongo = MongoClient(uri).ml4p

    # 1) Gather sources
    env_code = f'ENV_{country_code}'
    local_sources = [doc['source_domain'] for doc in db_mongo['sources'].find(
        {'primary_location': {'$in': [country_code]}, 'include': True},
        projection={'source_domain': 1})]
    env_local_sources = [doc['source_domain'] for doc in db_mongo['sources'].find(
        {'primary_location': {'$in': [env_code]}, 'include': True},
        projection={'source_domain': 1})]
    int_sources = [doc['source_domain'] for doc in db_mongo['sources'].find(
        {'$or': [{'major_international': True}, {'major_regional': True}], 'include': True},
        projection={'source_domain': 1})]

    # Deduplicate and avoid overlaps
    local_sources = sorted(set(local_sources))
    env_local_sources = sorted(set(env_local_sources))
    int_sources = sorted(set(int_sources) - set(local_sources) - set(env_local_sources))

    print(f"[{country_code}] Local: {len(local_sources)}, ENV_local: {len(env_local_sources)}, INT/REG: {len(int_sources)}")

    # 2) Count per domain in parallel
    loc_like_domains = local_sources + env_local_sources
    loc_args = [(uri, d, country_name, country_code) for d in loc_like_domains]
    int_args = [(uri, d, country_name, country_code) for d in int_sources]

    dfs_loc = p_umap(count_domain_loc_env, loc_args, num_cpus=num_cpus) if loc_args else []
    dfs_int = p_umap(count_domain_int_env, int_args, num_cpus=num_cpus) if int_args else []

    # Map domain -> df for later per-source normalization output
    domain_to_df = {}
    for d, df in zip(loc_like_domains, dfs_loc):
        domain_to_df[d] = df
    for d, df in zip(int_sources, dfs_int):
        domain_to_df[d] = df

    all_domain_dfs = list(domain_to_df.values())

    # 3) Aggregate raw across all sources (country-level raw)
    country_raw = sum_dfs(all_domain_dfs)

    # 4) Denominator: sum over sources of total_local_docs per month
    denom_docs = country_raw['total_local_docs'].astype('float64')
    denom_docs = denom_docs.mask(denom_docs == 0, np.nan)  # robust against zero

    # 5) Country-level normalization: per-month denom = TOTAL LOCAL DOCS (all sources)
    for label in category_labels:
        country_raw[label + '_norm'] = country_raw[label].astype('float64') / denom_docs

    # Restore year/month
    country_raw['year'] = country_raw.index.year
    country_raw['month'] = country_raw.index.month

    # 6) Write country-level (raw + _norm) to Counts_Env_Norm_Final
    out_country_dir = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Env_Norm/Final/{country_name}/{today.year}_{today.month}_{today.day}/'
    Path(out_country_dir).mkdir(parents=True, exist_ok=True)
    country_outfile = os.path.join(out_country_dir, f'{country_name}.csv')
    country_raw.sort_index().to_csv(country_outfile)
    print(f"[{country_code}] Country-level (raw + norm) written: {country_outfile}")

    # 7) Source-level normalized outputs (numeric columns except year/month),
    #    using the SAME cross-source denominator (TOTAL LOCAL DOCS).
    out_source_dir = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Env_Norm/By_Source/{country_name}/{today.year}_{today.month}_{today.day}/'
    Path(out_source_dir).mkdir(parents=True, exist_ok=True)

    for domain, df in domain_to_df.items():
        df_norm = df.copy()
        aligned = denom_docs.reindex(df_norm.index)

        # Select numeric columns and exclude year/month from normalization
        numeric_cols = [c for c in df_norm.columns
                        if pd.api.types.is_numeric_dtype(df_norm[c]) and c not in ('year', 'month')]

        # Cast to float to avoid integer NA issues; then divide
        df_norm[numeric_cols] = df_norm[numeric_cols].astype('float64').div(aligned, axis=0)

        # Keep same column names (no suffix) per request
        domain_outfile = os.path.join(out_source_dir, f'{domain}.csv')
        df_norm.sort_index().to_csv(domain_outfile)

    return country_outfile

# -----------------------------
# Git helper (unchanged)
# -----------------------------
def run_git_commands(commit_message):
    try:
        subprocess.run("git add *.py", shell=True, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

# -----------------------------
# Country list + main
# -----------------------------
if __name__ == "__main__":
    slp = False
    if slp:
        t = 7200
        print(f'start sleeping for {t/60} mins')
        time.sleep(t)

    # Base list (you only specify base codes like 'IND', 'PAK', 'ARM')
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

    # Specify base codes to process (no ENV_ codes needed)
    countries_needed = ['NIC']  # <-- edit this list as needed
    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

    for (country_name, country_code) in countries:
        print('Starting:', country_name)
        process_country(uri, country_name, country_code, num_cpus=10)
        try:
            commit_message = f"env classifier raw + normalized (country+source-level) ({country_code})"
            run_git_commands(commit_message)
        except Exception:
            pass

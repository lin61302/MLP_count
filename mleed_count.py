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
today = pd.Timestamp.now()
print(today)

# 1) Grab all possible environment labels from the DB, plus a 'total_articles' column.
env_labels = list(
    db.models.find_one({'model_name': 'env_classifier'}).get('event_type_nums').keys()
)
env_labels.append('total_articles')

################################################################
# We keep the Georgia location filters, as in your original code
################################################################

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

def check_georgia(doc_text, domain_type):
    """
    Returns False if the doc contains any of the forbidden 'CompanyName' patterns,
    meaning it's not truly relevant to Georgia. We keep exactly your original logic.
    """
    try:
        if domain_type == 'loc':
            return not g_loc_filter.search(doc_text)
        else:  # 'int'
            return not g_int_filter.search(doc_text)
    except:
        return True

################################################################
# New: Counting environment classifier results (env_max + env_sec)
################################################################

def count_domain_loc_env(uri, domain, country_name, country_code):
    """
    Count docs from 'local' sources for the specified country_code, 
    using the 'env_classifier' field. For each doc:
      - Increment df[env_max] and df[env_sec].
      - Also increment df['total_articles'].
    Keep Georgia-specific logic. Output CSV in the new path.
    """
    db_local = MongoClient(uri).ml4p

    # Prepare empty monthly DataFrame from 2012-1-1 to ~ now
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='MS')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    loc_code = country_code[-3:]

    # Add columns for each environment label + total_articles
    for label in env_labels:
        df[label] = 0

    # We'll fetch only needed fields
    projection_loc = {
        '_id': 1, 'env_classifier': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'cliff_locations': 1, 'en_cliff_locations': 1
    }

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        # Non-English docs
        cur1 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'environmental_binary.result': 'Yes',
                # We want env_classifier to exist
                'env_classifier': {'$exists': True},
                'language': {'$ne': 'en'},
                # location-based filter
                '$or': [
                    {f'cliff_locations.{loc_code}': {'$exists': True}},
                    {'cliff_locations': {}}
                ]
            },
            projection=projection_loc,
            batch_size=100
        )
        docs1 = list(cur1)

        # English docs
        cur2 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'environmental_binary.result': 'Yes',
                'env_classifier': {'$exists': True},
                'language': 'en',
                '$or': [
                    {f'en_cliff_locations.{loc_code}': {'$exists': True}},
                    {'en_cliff_locations': {}}
                ]
            },
            projection=projection_loc,
            batch_size=100
        )
        docs2 = list(cur2)
        docs = docs1 + docs2

        if not docs:
            continue

        # If country_code == 'GEO', we do Georgia text filtering
        if country_code == 'GEO':
            filtered_docs = []
            for d in docs:
                # Combine text from title + main, or do them separately
                title_t = d.get('title_translated','')
                main_t = d.get('maintext_translated','')
                # Must pass check_georgia in 'loc' mode
                if check_georgia(main_t, 'loc') and check_georgia(title_t, 'loc'):
                    filtered_docs.append(d)
            docs = filtered_docs

        # Count each doc
        for d in docs:
            # For date-based indexing
            # If date_publish fails, parse fallback
            try:
                doc_date = pd.Timestamp(d['date_publish'].year, d['date_publish'].month, 1)
            except:
                dd = dateparser.parse(d['date_publish']).replace(tzinfo=None)
                doc_date = pd.Timestamp(dd.year, dd.month, 1)

            env_info = d.get('env_classifier')
            if not env_info:
                # Should not happen given $exists: True, but just in case
                continue

            # Grab env_max, env_sec
            env_max = env_info.get('env_max', None)
            env_sec = env_info.get('env_sec', None)

            # Possibly skip if they are None or '-999', but user says "double-count is ok"
            # so let's just count them as is, if they're real labels
            if env_max in env_labels:
                df.loc[doc_date, env_max] += 1
            if env_sec in env_labels:
                df.loc[doc_date, env_sec] += 1

            # Always increment total_articles for any doc
            df.loc[doc_date, 'total_articles'] += 1

            # If country_code == 'GEO', optionally mark a DB field
            # (like your original approach with "Country_Georgia")
            if country_code == 'GEO':
                colname_g = f"articles-{doc_date.year}-{doc_date.month}"
                # Mark in DB if you want. We replicate your pattern:
                db_local[colname_g].update_one(
                    {'_id': d['_id']},
                    {'$set': {'Country_Georgia': 'Yes'}}
                )

    # Finally, write the CSV output
    out_path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Env/{country_name}/{today.year}_{today.month}_{today.day}/'
    Path(out_path).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(out_path, f'{domain}.csv'))


def count_domain_int_env(uri, domain, country_name, country_code):
    """
    Same logic as count_domain_loc_env, but now it's 'international' articles
    that must have cliff_locations.<country_code> or en_cliff_locations.<country_code> 
    strictly present (no empty dict). We keep the original approach from your script.
    """
    db_local = MongoClient(uri).ml4p

    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='MS')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    loc_code = country_code[-3:]

    for label in env_labels:
        df[label] = 0

    projection_int = {
        '_id': 1, 'env_binary': 1, 'env_classifier': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'cliff_locations': 1, 'en_cliff_locations': 1
    }

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        # Non-English docs
        cur1 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'environmental_binary.result': 'Yes',
                'env_classifier': {'$exists': True},
                'language': {'$ne': 'en'},
                f'cliff_locations.{loc_code}': {'$exists': True}
            },
            projection=projection_int,
            batch_size=100
        )
        docs1 = list(cur1)

        # English docs
        cur2 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'environmental_binary.result': 'Yes',
                'env_classifier': {'$exists': True},
                'language': 'en',
                f'en_cliff_locations.{loc_code}': {'$exists': True}
            },
            projection=projection_int,
            batch_size=100
        )
        docs2 = list(cur2)

        docs = docs1 + docs2
        if not docs:
            continue

        # If GEO, filter text using the "int" pattern
        if country_code == 'GEO':
            filtered_docs = []
            for d in docs:
                title_t = d.get('title_translated','')
                main_t = d.get('maintext_translated','')
                if check_georgia(main_t, 'int') and check_georgia(title_t, 'int'):
                    filtered_docs.append(d)
            docs = filtered_docs

        for d in docs:
            try:
                doc_date = pd.Timestamp(d['date_publish'].year, d['date_publish'].month, 1)
            except:
                dd = dateparser.parse(d['date_publish']).replace(tzinfo=None)
                doc_date = pd.Timestamp(dd.year, dd.month, 1)

            env_info = d.get('env_classifier')
            if not env_info:
                continue

            env_max = env_info.get('env_max', None)
            env_sec = env_info.get('env_sec', None)

            if env_max in env_labels:
                df.loc[doc_date, env_max] += 1
            if env_sec in env_labels:
                df.loc[doc_date, env_sec] += 1

            df.loc[doc_date, 'total_articles'] += 1

            if country_code == 'GEO':
                colname_g = f"articles-{doc_date.year}-{doc_date.month}"
                db_local[colname_g].update_one(
                    {'_id': d['_id']},
                    {'$set': {'Country_Georgia': 'Yes'}}
                )

    out_path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Env/{country_name}/{today.year}_{today.month}_{today.day}/'
    Path(out_path).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(out_path, f'{domain}.csv'))

def run_git_commands(commit_message):
    try:
        subprocess.run("git add *.py", shell=True, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

if __name__ == "__main__":

    # Example: just for 'Panama' (PAN)
    countries_needed = ['GTM','ENV_GTM']
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
        ('Environmental International','ENV_INT'),
        ('Environmental Nicaragua','ENV_NIC'),
        ('Environmental Solomon Islands','ENV_SLB'),
        ('Environmental Nigeria','ENV_NGA'),
        ('Environmental El Salvador','ENV_SLV'),
        ('Environmental Guatemala','ENV_GTA'),
        ('Environmental Panama','ENV_PAN'),
        ('Environmental Costa Rica','ENV_CRI'),
        ('Environmental Belarus','ENV_BLR'),
        ('Environmental Burkina Faso','ENV_BFA'),
        ('Environmental Albania','ENV_ALB'),
        ('Environmental Angola','ENV_AGO'),
        ('Environmental Nigeria','ENV_NGA'),
        ('Environmental El Salvado','ENV_SLV'),
        ('Environmental Benin','ENV_BEN'),
        ('Environmental Pakistan','ENV_PAK'),
        ('Environmental Honduras','ENV_HND'),
    ]

    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

    for (country_name, country_code) in countries:
        print('Starting:', country_name)

        # Query your DB for local, international, major_regional sources
        db_mongo = MongoClient(uri).ml4p
        loc = [doc['source_domain'] for doc in db_mongo['sources'].find(
            {'primary_location': {'$in':[country_code]}, 'include': True}
        )]
        ints = [doc['source_domain'] for doc in db_mongo['sources'].find({'major_international': True, 'include': True})]
        regionals = [doc['source_domain'] for doc in db_mongo['sources'].find({'major_regional': True, 'include': True})]
        mlp_int = ints + regionals

        env_ints = [doc['source_domain'] for doc in db_mongo['sources'].find(
            {'primary_location': {'$in':['ENV_INT']}, 'include': True}
        )]

        # For local sources
        p_umap(count_domain_loc_env, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)

        # For international and regionals sources
        p_umap(count_domain_int_env, [uri]*len(mlp_int), ints, [country_name]*len(mlp_int), [country_code]*len(mlp_int), num_cpus=10)

        # For environmental international and regionals sources
        p_umap(count_domain_int_env, [uri]*len(env_ints), env_ints, [country_name]*len(env_ints), [country_code]*len(env_ints), num_cpus=10)

        commit_message = f"env classifier count ({country_code}) update"
        run_git_commands(commit_message)

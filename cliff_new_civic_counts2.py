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
print(today)

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

def count_domain_loc(uri, domain, country_name, country_code):

    db_local = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    for et in events:
        df[et] = 0

    # We'll fetch only needed fields in projection
    projection_loc = {
        '_id': 1, 'civic_new': 1, 'date_publish': 1,
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
                'civic_new': {'$exists': True},
                'language': {'$ne': 'en'},
                '$or': [
                    {f'cliff_locations.{country_code}': {'$exists': True}},
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
                'civic_new': {'$exists': True},
                'language': 'en',
                '$or': [
                    {f'en_cliff_locations.{country_code}': {'$exists': True}},
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

        # For each event, same logic as before
        for et in events:
            if et == 'coup':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'legalaction':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_coup(d)]
                sub_docs = sub_docs1 + sub_docs2

            elif et == 'legalaction':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_corruption_LA(d) and not check_corruption_PU(d)]
                sub_docs = sub_docs1 + sub_docs2

            elif et == 'arrest':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_corruption_AR(d) and not check_corruption_PU(d) and not check_corruption_LA(d)]
                sub_docs = sub_docs1 + sub_docs2

            elif et == 'purge':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_corruption_PU(d)]
                sub_docs = sub_docs1 + sub_docs2

            elif et == 'defamationcase':
                sub_docs = []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == 'legalaction':
                            sub_docs.append(d)
                    except:
                        pass
                sub_docs = [d for d in sub_docs if check_defamation(d) and not check_coup(d)]

            elif et == 'censor':
                sub_docs = []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs.append(d)
                    except:
                        pass
                sub_docs = [d for d in sub_docs if check_censorship(d)]

            elif et == 'corruption':
                sub_docs, sub_docs1, sub_docs2, sub_docs3 = [], [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] in ['arrest','purge']:
                            sub_docs2.append(d)
                        if d['civic_new']['event_type'] == 'legalaction':
                            sub_docs3.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_double(d)]
                sub_docs3 = [d for d in sub_docs3 if check_double(d) and not check_defamation(d) and not check_coup(d)]
                sub_docs = sub_docs1 + sub_docs2 + sub_docs3

            elif et in ['violencelethal','violencenonlethal']:
                sub_docs1 = []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                    except:
                        pass
                sub_docs = [d for d in sub_docs1 if (check_ukr(d) and country_code in d.get('cliff_locations', {})) or (not check_ukr(d))]
                docs_ukr = [d for d in sub_docs1 if check_ukr(d)]
                # Start a process to add Ukraine
                proc = multiprocessing.Process(target=add_ukr(docs_ukr))
                proc.start()

            elif et == '-999':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == '-999':
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'censor':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if not check_censorship(d)]
                sub_docs = sub_docs1 + sub_docs2

            else:
                # default
                sub_docs = []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs.append(d)
                    except:
                        pass

            if country_code == 'GEO':
                sub_docs = [d for d in sub_docs if check_georgia(d.get('maintext_translated',''), 'loc')
                            and check_georgia(d.get('title_translated',''), 'loc')]

            if et == 'total_articles':
                count_val = len(docs)
            else:
                count_val = len(sub_docs)

            df.loc[date, et] = count_val

        if country_code == 'GEO':
            for _doc in docs:
                try:
                    colname_g = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                except:
                    dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
                    colname_g = f"articles-{dd.year}-{dd.month}"
                is_yes = check_georgia(_doc.get('maintext_translated',''), 'loc') and \
                         check_georgia(_doc.get('title_translated',''), 'loc')
                db_local[colname_g].update_one(
                    {'_id': _doc['_id']},
                    {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}}
                )

        # Original event_types
        event_types = [d['civic_new']['event_type'] for d in docs]
        event_types2 = [None]*len(docs)

        for idx, d in enumerate(docs):
            e_type = d['civic_new']['event_type']
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
                    event_types[idx] = '-999'

            elif e_type == 'arrest':
                if check_double(d):
                    event_types[idx] = 'arrest'
                    event_types2[idx] = 'corruption'

            elif e_type == 'purge':
                if check_double(d):
                    event_types[idx] = 'purge'
                    event_types2[idx] = 'corruption'

            elif e_type == 'corruption':
                if check_corruption_PU(d):
                    event_types[idx] = 'corruption'
                    event_types2[idx] = 'purge'
                elif check_corruption_LA(d):
                    event_types[idx] = 'corruption'
                    event_types2[idx] = 'legalaction'
                elif check_corruption_AR(d):
                    event_types[idx] = 'corruption'
                    event_types2[idx] = 'arrest'
                else:
                    event_types[idx] = 'corruption'
            else:
                event_types[idx] = e_type

        proc2 = multiprocessing.Process(
            target=update_info(docs=docs, event_types=event_types, event_types2=event_types2, colname=colname)
        )
        proc2.start()

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    Path(path).mkdir(parents=True, exist_ok=True)
    df.to_csv(os.path.join(path, f'{domain}.csv'))

def count_domain_int(uri, domain, country_name, country_code):
    db_local = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')
    df.index = df['date']
    df['year'] = df.index.year
    df['month'] = df.index.month

    for et in events:
        df[et] = 0

    projection_int = {
        '_id': 1, 'civic_new': 1, 'date_publish': 1,
        'title_translated': 1, 'maintext_translated': 1,
        'cliff_locations': 1, 'en_cliff_locations': 1
    }

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        cur1 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': {'$ne': 'en'},
                f'cliff_locations.{country_code}': {'$exists': True}
            },
            projection=projection_int,
            batch_size=100
        )
        docs1 = list(cur1)

        cur2 = db_local[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': 'en',
                f'en_cliff_locations.{country_code}': {'$exists': True}
            },
            projection=projection_int,
            batch_size=100
        )
        docs2 = list(cur2)
        docs = docs1 + docs2

        if not docs:
            continue

        for et in events:
            if et == 'coup':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'legalaction':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_coup(d)]
                sub_docs = sub_docs1 + sub_docs2

            elif et == 'legalaction':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_corruption_LA(d) and not check_corruption_PU(d)]
                sub_docs = sub_docs1 + sub_docs2

            elif et == 'arrest':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_corruption_AR(d) and not check_corruption_PU(d) and not check_corruption_LA(d)]
                sub_docs = sub_docs1 + sub_docs2

            elif et == 'purge':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_corruption_PU(d)]
                sub_docs = sub_docs1 + sub_docs2

            elif et == 'defamationcase':
                sub_docs = []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == 'legalaction':
                            sub_docs.append(d)
                    except:
                        pass
                sub_docs = [d for d in sub_docs if check_defamation(d) and not check_coup(d)]

            elif et == 'censor':
                sub_docs = []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs.append(d)
                    except:
                        pass
                sub_docs = [d for d in sub_docs if check_censorship(d)]

            elif et == 'corruption':
                sub_docs, sub_docs1, sub_docs2, sub_docs3 = [], [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] in ['arrest','purge']:
                            sub_docs2.append(d)
                        if d['civic_new']['event_type'] == 'legalaction':
                            sub_docs3.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if check_double(d)]
                sub_docs3 = [d for d in sub_docs3 if check_double(d) and not check_defamation(d) and not check_coup(d)]
                sub_docs = sub_docs1 + sub_docs2 + sub_docs3

            elif et == '-999':
                sub_docs, sub_docs1, sub_docs2 = [], [], []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == '-999':
                            sub_docs1.append(d)
                        if d['civic_new']['event_type'] == 'censor':
                            sub_docs2.append(d)
                    except:
                        pass
                sub_docs2 = [d for d in sub_docs2 if not check_censorship(d)]
                sub_docs = sub_docs1 + sub_docs2

            else:
                sub_docs = []
                for d in docs:
                    try:
                        if d['civic_new']['event_type'] == et:
                            sub_docs.append(d)
                    except:
                        pass

            if country_code == 'GEO':
                sub_docs = [d for d in sub_docs
                            if check_georgia(d.get('maintext_translated',''), 'int')
                            and check_georgia(d.get('title_translated',''), 'int')]

            count_val = len(docs) if et == 'total_articles' else len(sub_docs)
            df.loc[date, et] = count_val

        if country_code == 'GEO':
            for _doc in docs:
                try:
                    colname_g = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                except:
                    dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
                    colname_g = f"articles-{dd.year}-{dd.month}"
                is_yes = check_georgia(_doc.get('maintext_translated',''), 'int') and \
                         check_georgia(_doc.get('title_translated',''), 'int')
                db_local[colname_g].update_one(
                    {'_id': _doc['_id']},
                    {'$set': {'Country_Georgia': 'Yes' if is_yes else 'No'}}
                )

        event_types = [d['civic_new']['event_type'] for d in docs]
        event_types2 = [None]*len(docs)
        for idx, d in enumerate(docs):
            e_type = d['civic_new']['event_type']
            if e_type == 'legalaction':
                if check_coup(d):
                    event_types[idx] = 'legalaction'
                    event_types2[idx] = 'coup'
                elif check_defamation(d):
                    event_types[idx] = 'legalaction'
                    event_types2[idx] = 'defamationcase'
                else:
                    if check_double(d):
                        event_types2[idx] = 'corruption'
            elif e_type == 'censor':
                if check_censorship(d):
                    event_types[idx] = 'censor'
                else:
                    event_types[idx] = '-999'
            elif e_type == 'arrest':
                if check_double(d):
                    event_types[idx] = 'arrest'
                    event_types2[idx] = 'corruption'
            elif e_type == 'purge':
                if check_double(d):
                    event_types[idx] = 'purge'
                    event_types2[idx] = 'corruption'
            elif e_type == 'corruption':
                if check_corruption_PU(d):
                    event_types[idx] = 'corruption'
                    event_types2[idx] = 'purge'
                elif check_corruption_LA(d):
                    event_types[idx] = 'corruption'
                    event_types2[idx] = 'legalaction'
                elif check_corruption_AR(d):
                    event_types[idx] = 'corruption'
                    event_types2[idx] = 'arrest'

        proc2 = multiprocessing.Process(
            target=update_info(docs=docs, event_types=event_types, event_types2=event_types2, colname=colname)
        )
        proc2.start()

    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
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
        ('Panama','PAN')
        
    ]
    

    countries_needed = [
        
                            # 'COL', 'ECU',  'PRY','JAM','HND', 'SLV', 'NIC','PER', 'DOM','PAN', 'CRI','SLB', 
                            # 'BGD','NGA','UGA',
                            #    'ALB', 'BEN', 'ETH', 'GEO', 'KEN', 'MLI', 'MAR',   
                            #    'SRB', 'SEN', 'TZA', 'UKR', 'ZWE', 'MRT', 'ZMB', 'XKX', 'NER',  
                            #     'PHL', 'GHA', 'RWA', 'GTM', 'BLR', 'KHM', 'COD', 'TUR', 
                            #    'ZAF', 'TUN', 'IDN', 'AGO', 'ARM', 'LKA', 'MYS', 'CMR', 'HUN', 'MWI', 
                               'UZB', 'IND', 'MOZ', 'AZE', 'KGZ', 'MDA', 'KAZ', 'DZA', 'MKD', 'SSD', 
                            #    'LBR', 'PAK', 'NPL', 'NAM', 'BFA', 'TLS', #'MEX'
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
                p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)
                ind = 0
            except Exception as err:
                print(err)
                pass

        ind = 1
        while ind:
            try:
                p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=10)
                ind = 0
            except Exception as err:
                print(err)
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
        commit_message = f"civic count ({countries_added}) update"
        run_git_commands(commit_message)

# screen -S screen_count
# screen -r screen_count
# conda activate peace

# cd /home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts
#'ETH','TZA','BEN','COL','ECU','DZA','NIC','KEN','JAM','GTM','MLI','SEN','ZWE','COD'

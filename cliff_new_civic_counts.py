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

load_dotenv()
#uri = os.getenv('DATABASE_URL')
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p
# (?<![a-zA-Z])
today = pd.Timestamp.now()

events = [k for k in db.models.find_one({'model_name': 'civic_new'}).get('event_type_nums').keys()] + ['defamationcase'] +['total_articles']

# legal_re = re.compile(r'((?<![a-zA-Z])freedom.*|(?<![a-zA-Z])assembl.*|(?<![a-zA-Z])associat.*|(?<![a-zA-Z])term limit.*|(?<![a-zA-Z])independen.*|(?<![a-zA-Z])succession.*|(?<![a-zA-Z])demonstrat.*|(?<![a-zA-Z])crackdown.*|(?<![a-zA-Z])draconian|(?<![a-zA-Z])censor.*|(?<![a-zA-Z])authoritarian|(?<![a-zA-Z])repress.*|(?<![a-zA-Z])NGO(?<![a-zA-Z])|(?<![a-zA-Z])human rights)',flags=re.IGNORECASE)

censor_re = re.compile(r'\b(freedom\w*|assembl\w*|associat\w*|term limit\w*|independen\w*|succession\w*|demonstrat\w*|crackdown\w*|draconian|censor\w*|authoritarian|repress\w*|NGO\b|human rights|journal\w*|newspaper|media|outlet|reporter|broadcast\w*|correspondent|press|magazine|paper|black out|blacklist|suppress|speaking|false news|fake news|radio|commentator|blogger|opposition voice|voice of the opposition|speech|publish)\b', flags=re.IGNORECASE)

defame_re1 = re.compile(r'\b(case|lawsuit|sue|suing|suit|trial|court|charge\w*|rule|ruling|sentence|sentencing|judg\w*)\b', flags=re.IGNORECASE)
defame_re2 = re.compile(r'\b(defamation|defame|libel|slander|insult|reputation|lese majeste|lese majesty|lese-majeste)\b', flags=re.IGNORECASE)

double_re = re.compile(r'\b(embezzle\w*|bribe\w*|gift\w*|fraud\w*|corrupt\w*|procure\w*|budget|assets|irregularities|graft|enrich\w*|laundering)\b', flags=re.IGNORECASE)

corrupt_LA_re = re.compile(r'\b(legal process|case|investigat\w*|appeal|prosecut\w*|lawsuit|sue|suing|trial|court|charg\w*|rule|ruling|sentenc\w*|judg\w*)\b', flags=re.IGNORECASE)
corrupt_AR_re = re.compile(r'\b(arrest|detain|apprehend|captur\w*|custod\w*|imprison|jail)\b', flags=re.IGNORECASE)
corrupt_PU_re = re.compile(r'\b(resign|fire|firing|dismiss|sack|replac\w*|quit)\b', flags=re.IGNORECASE)

coup_re = re.compile(r'((?<![a-zA-Z])coup(?<![a-zA-Z])|(?<![a-zA-Z])coups(?<![a-zA-Z])|(?<![a-zA-Z])depose|(?<![a-zA-Z])overthrow|(?<![a-zA-Z])oust)')

ukr_re = re.compile(r'(ukrain.*)',flags=re.IGNORECASE)

# For arrest: apprehend, captur*, custody, imprison, jail
# For legal action: case, lawsuit, sue, suit, trial, court, charge, rule, sentence, judge
# For purge: dismiss, sack, replace, quit

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
g_int_filter = re.compile(g_int_string,flags=re.IGNORECASE)
g_loc_filter = re.compile(g_loc_string,flags=re.IGNORECASE)

def check_georgia(doc, _domain):  
    if _domain == 'loc':
        try:
            if bool(g_loc_filter.search(doc)):
                return False
            else:
                return True
        except:
            return True
    if _domain == 'int':
        try:
            if bool(g_int_filter.search(doc)):
                return False
            else:
                return True
        except:
            return True


# def check_legal(doc):
#     try:
#         if bool(legal_re.search(doc.get('title_translated'))) or bool(legal_re.search(doc.get('maintext_translated'))):
#             return True
#         else:
#             return False
#     except:
#         return False

def check_censorship(doc):
    try:
        if bool(censor_re.search(doc.get('title_translated'))) or bool(censor_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False

def check_defamation(doc):
    try:
        if bool(bool(defame_re2.search(doc.get('title_translated'))) or bool(defame_re2.search(doc.get('maintext_translated')))) and bool(bool(defame_re1.search(doc.get('title_translated'))) or bool(defame_re1.search(doc.get('maintext_translated')))):
            return True
        else:
            return False
    except:
        return False
    
    
def check_double(doc):
    try:
        if bool(double_re.search(doc.get('title_translated'))) or bool(double_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False
    
def check_corruption_LA(doc):
    try:
        if bool(corrupt_LA_re.search(doc.get('title_translated'))) or bool(corrupt_LA_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False
    
def check_corruption_AR(doc):
    try:
        if bool(corrupt_AR_re.search(doc.get('title_translated'))) or bool(corrupt_AR_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False

def check_corruption_PU(doc):
    try:
        if bool(corrupt_PU_re.search(doc.get('title_translated'))) or bool(corrupt_PU_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False
    
def check_coup(doc):
    try:
        if bool(coup_re.search(doc.get('title_translated'))) or bool(coup_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False

def check_ukr(doc):  
    try:
        if bool(ukr_re.search(doc.get('title_translated'))) or bool(ukr_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False
    

def update_info(docs, event_types, event_types2, colname):
    """
    updates the docs into the db
    """
    db = MongoClient(uri).ml4p

    for nn, _doc in enumerate(docs):
        try:
            colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        except:
            dd = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
            colname = f"articles-{dd.year}-{dd.month}"
        db[colname].update_one(
            {
                '_id': _doc['_id']
            },
            {
                '$set':{
                    'event_type_civic_new':event_types[nn],
                    'event_type_civic_new_2':event_types2[nn]
                            
                }
            }
        )

def add_ukr(docs_ukr):
    """
    updates the docs into the db
    """
    db = MongoClient(uri).ml4p

    for nn, _doc in enumerate(docs_ukr):
        try:
            colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        except:
            dd = dateparser.parse(_doc['date_publish']).replace(tzinfo=None)
            colname = f"articles-{dd.year}-{dd.month}"

        existing_doc = db[colname].find_one({'_id': _doc['_id']})
        if existing_doc:
            cliff_locations = existing_doc.get('cliff_locations', {})
            if 'UKR' in cliff_locations:
                if 'Ukraine' not in cliff_locations['UKR']:
                    cliff_locations['UKR'].insert(0, 'Ukraine')
            else:
                cliff_locations['UKR'] = ['Ukraine']
            db[colname].update_one(
                {'_id': _doc['_id']},
                {'$set': {'cliff_locations': cliff_locations}}
            )
        else:
            db[colname].update_one(
                {'_id': _doc['_id']},
                {'$set': {'cliff_locations.UKR': ['Ukraine']}}
            )

# START WITH THE LOCALS
def count_domain_loc(uri, domain, country_name, country_code):

    db = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]

    for et in events:
        df[et] = [0] * len(df)

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"
        

        # cur = db[colname].find(
        #     {
        #         'source_domain': domain,
        #         'include': True,
        #         'civic_new': {'$exists': True},
        #         '$or': [
        #             {'cliff_locations.' + country_code : {'$exists' : True}},
        #             {'cliff_locations' : {}}
        #         ]
        #     }
        # )
        # docs = [doc for doc in cur]

        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': {'$ne': 'en'},
                '$or': [
                    {'cliff_locations.' + country_code : {'$exists' : True}},
                    {'cliff_locations' : {}}
                ]
            }, batch_size=1
        )
        docs1 = [doc for doc in cur1]

        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': 'en'  ,
                '$or': [
                    {'en_cliff_locations.' + country_code : {'$exists' : True}},
                    {'en_cliff_locations' : {}}
                ]
            }, batch_size=1
        )

        docs2 = [doc for doc in cur2]
        docs = docs1+docs2

        for et in events:

            if et == 'coup':
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'legalaction':
                            sub_docs2.append(doc)
                    except:
                        print(doc)

                sub_docs2 = [doc for doc in sub_docs2 if (check_coup(doc))]
                
                sub_docs = sub_docs1 + sub_docs2


            elif et == 'legalaction':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'legalaction']
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(doc)
                    except:
                        print(doc)
                # sub_docs1 = [doc for doc in sub_docs1 if not check_defamation(doc)]
                sub_docs2 = [doc for doc in sub_docs2 if (check_corruption_LA(doc)) and (not check_corruption_PU(doc))]
                
                sub_docs = sub_docs1 + sub_docs2
            
            elif et == 'arrest':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'legalaction']
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                sub_docs3 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(doc)
                    except:
                        print(doc)
                
                sub_docs2 = [doc for doc in sub_docs2 if check_corruption_AR(doc) and (not check_corruption_PU(doc)) and (not check_corruption_LA(doc))]        
                sub_docs = sub_docs1 + sub_docs2
                # sub_docs = [doc for doc in sub_docs3 if (check_ukr(doc) and country_code in doc['cliff_locations']) or (not check_ukr(doc))]
                # docs_ukr = [doc for doc in docs if check_ukr(doc)] 
                
            elif et == 'purge':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'legalaction']
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(doc)
                    except:
                        print(doc)
                
                sub_docs2 = [doc for doc in sub_docs2 if check_corruption_PU(doc)]        
                sub_docs = sub_docs1 + sub_docs2
                
            
            elif et == 'defamationcase':
                sub_docs = []
                
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == 'legalaction':
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if (check_defamation(doc)) and not (check_coup(doc))]
                
                
                
            
            elif et == 'censor':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'censor']
                sub_docs = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if check_censorship(doc)]
                
                
                
            elif et == 'corruption':
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                sub_docs3 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                            
                        if doc['civic_new']['event_type'] == 'arrest' or doc['civic_new']['event_type'] == 'purge':
                            sub_docs2.append(doc)
                            
                        if doc['civic_new']['event_type'] == 'legalaction':
                            sub_docs3.append(doc)
          
                    except:
                        print(doc)
                        
                sub_docs2 = [doc for doc in sub_docs2 if check_double(doc)]
                sub_docs3 = [doc for doc in sub_docs3 if (check_double(doc)) and not (check_defamation(doc)) and not (check_coup(doc))]
                sub_docs = sub_docs1 + sub_docs2 + sub_docs3
            
            elif et =='violencelethal' or et == 'violencenonlethal':
                sub_docs = []
                sub_docs1 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                    except:
                        print(doc)

                sub_docs = [doc for doc in sub_docs1 if (check_ukr(doc) and country_code in doc['cliff_locations']) or (not check_ukr(doc))]
                docs_ukr = [doc for doc in sub_docs1 if check_ukr(doc)]
                proc = multiprocessing.Process(target=add_ukr(docs_ukr))
                proc.start()

                
            elif et == '-999':
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'censor':
                            sub_docs2.append(doc)
                    except:
                        print(doc)
                
                sub_docs2 = [doc for doc in sub_docs2 if not check_censorship(doc)]        
                sub_docs = sub_docs1 + sub_docs2


            else: 
                sub_docs = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                        
                            sub_docs.append(doc)
                    except:
                        print(doc)


            if country_code == 'GEO':
                sub_docs = [doc for doc in sub_docs if check_georgia(doc['maintext_translated'], _domain='loc') and check_georgia(doc['title_translated'], _domain='loc')]
                


            
            if et == 'total_articles':
                count = len(docs)

            else:
                count = len(sub_docs)


            df.loc[date, et] = count
        
        if country_code == 'GEO':
            for nn, _doc in enumerate(docs):
                    try:
                        colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                    except:
                        dd = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
                        colname = f"articles-{dd.year}-{dd.month}"
                    if check_georgia(_doc['maintext_translated'], _domain='loc') and check_georgia(_doc['title_translated'], _domain='loc'):
                        db[colname].update_one(
                            {
                                '_id': _doc['_id']
                            },
                            {
                                '$set':{
                                    'Country_Georgia': 'Yes'
                                            
                                }
                            }
                            )
                    else:
                        db[colname].update_one(
                            {
                                '_id': _doc['_id']
                            },
                            {
                                '$set':{
                                    'Country_Georgia': 'No'
                                            
                                }
                            }
                            )


        #original event type
        event_types = [doc['civic_new']['event_type'] for doc in docs]
        event_types2 = [None] * len(docs)
        #update event type based on filter outcome
        for index, doc in enumerate(docs):

            if doc['civic_new']['event_type'] == 'legalaction':
                if check_coup(doc):
                    event_types[index] = 'legalaction'
                    event_types2[index] = 'coup'
                elif check_defamation(doc):
                    event_types[index] = 'legalaction'
                    event_types2[index] = 'defamationcase'
                else:
                    event_types[index] = 'legalaction'
                    if check_double(doc):
                        event_types2[index] = 'corruption'

            elif doc['civic_new']['event_type'] == 'censor':
                if check_censorship(doc):
                    event_types[index] = 'censor'
                else: 
                    event_types[index] = '-999'
                    
            elif doc['civic_new']['event_type'] == 'arrest':
                if check_double(doc):
                    event_types[index] = 'arrest'
                    event_types2[index] = 'corruption'
                else:
                    event_types[index] = 'arrest'
            
            elif doc['civic_new']['event_type'] == 'purge':
                if check_double(doc):
                    event_types[index] = 'purge'
                    event_types2[index] = 'corruption'
                else:
                    event_types[index] = 'purge'
            
            elif doc['civic_new']['event_type'] == 'corruption':
                if check_corruption_PU(doc):
                    event_types[index] = 'corruption'
                    event_types2[index] = 'purge'
                elif check_corruption_LA(doc):
                    event_types[index] = 'corruption'
                    event_types2[index] = 'legalaction'
                elif check_corruption_AR(doc):
                    event_types[index] = 'corruption'
                    event_types2[index] = 'arrest'
                else:
                    event_types[index] = 'corruption'
       

            else:
                event_types[index] = doc['civic_new']['event_type']

        

        #update data with new event_types
        proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, event_types2 = event_types2, colname = colname))
        proc.start()

            




    # check if directory exists
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)
    df.to_csv(path + f'{domain}.csv')


# Then ints
def count_domain_int(uri, domain, country_name, country_code):

    db = MongoClient(uri).ml4p

    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]

    for et in events:
        df[et] = [0] * len(df)

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        # cur = db[colname].find(
        #     {
        #         'source_domain': domain,
        #         'include': True,
        #         'civic_new': {'$exists': True},
        #         'cliff_locations.' + country_code : {'$exists' : True},
        #     }, batch_size=1
        # )
        # docs = [doc for doc in cur]

        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': {'$ne': 'en'},
                'cliff_locations.' + country_code : {'$exists' : True}, 
            }, batch_size=1
        )
        docs1 = [doc for doc in cur1]

        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': 'en'  ,
                'en_cliff_locations.' + country_code : {'$exists' : True},
            }, batch_size=1
        )

        docs2 = [doc for doc in cur2]
        docs = docs1+docs2

 
        for et in events:

            if et == 'coup':
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'legalaction':
                            sub_docs2.append(doc)
                    except:
                        print(doc)

                sub_docs2 = [doc for doc in sub_docs2 if (check_coup(doc))]
                
                sub_docs = sub_docs1 + sub_docs2


            elif et == 'legalaction':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'legalaction']
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(doc)
                    except:
                        print(doc)
                # sub_docs1 = [doc for doc in sub_docs1 if not check_defamation(doc)]
                sub_docs2 = [doc for doc in sub_docs2 if (check_corruption_LA(doc)) and (not check_corruption_PU(doc))]
                
                sub_docs = sub_docs1 + sub_docs2
            
            elif et == 'arrest':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'legalaction']
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                sub_docs3 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(doc)
                    except:
                        print(doc)
                
                sub_docs2 = [doc for doc in sub_docs2 if check_corruption_AR(doc) and (not check_corruption_PU(doc)) and (not check_corruption_LA(doc))]        
                sub_docs = sub_docs1 + sub_docs2
                # sub_docs = [doc for doc in sub_docs3 if (check_ukr(doc) and country_code in doc['cliff_locations']) or (not check_ukr(doc))]
                # docs_ukr = [doc for doc in docs if check_ukr(doc)] 
                
            elif et == 'purge':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'legalaction']
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'corruption':
                            sub_docs2.append(doc)
                    except:
                        print(doc)
                
                sub_docs2 = [doc for doc in sub_docs2 if check_corruption_PU(doc)]        
                sub_docs = sub_docs1 + sub_docs2
                
            
            elif et == 'defamationcase':
                sub_docs = []
                
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == 'legalaction':
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if (check_defamation(doc)) and not (check_coup(doc))]
                
                
                
            
            elif et == 'censor':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'censor']
                sub_docs = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if check_censorship(doc)]
                
                
                
            elif et == 'corruption':
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                sub_docs3 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                            
                        if doc['civic_new']['event_type'] == 'arrest' or doc['civic_new']['event_type'] == 'purge':
                            sub_docs2.append(doc)
                            
                        if doc['civic_new']['event_type'] == 'legalaction':
                            sub_docs3.append(doc)
          
                    except:
                        print(doc)
                        
                sub_docs2 = [doc for doc in sub_docs2 if check_double(doc)]
                sub_docs3 = [doc for doc in sub_docs3 if (check_double(doc)) and not (check_defamation(doc)) and not (check_coup(doc))]
                sub_docs = sub_docs1 + sub_docs2 + sub_docs3
                
            elif et == '-999':
                sub_docs = []
                sub_docs1 = []
                sub_docs2 = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                            sub_docs1.append(doc)
                        if doc['civic_new']['event_type'] == 'censor':
                            sub_docs2.append(doc)
                    except:
                        print(doc)
                
                sub_docs2 = [doc for doc in sub_docs2 if not check_censorship(doc)]        
                sub_docs = sub_docs1 + sub_docs2


            else: 
                sub_docs = []
                for doc in docs:
                    try:
                        if doc['civic_new']['event_type'] == et:
                        
                            sub_docs.append(doc)
                    except:
                        print(doc)
                        
                
            if country_code == 'GEO':
                sub_docs = [doc for doc in sub_docs if check_georgia(doc['maintext_translated'], _domain='int') and check_georgia(doc['title_translated'], _domain='int')]
            
            if et == 'total_articles':
                count = len(docs)

            else:
                count = len(sub_docs)

            df.loc[date, et] = count

        if country_code == 'GEO':
            for nn, _doc in enumerate(docs):
                    try:
                        colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
                    except:
                        dd = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
                        colname = f"articles-{dd.year}-{dd.month}"
                    if check_georgia(_doc['maintext_translated'], _domain='int') and check_georgia(_doc['title_translated'], _domain='int'):
                        db[colname].update_one(
                            {
                                '_id': _doc['_id']
                            },
                            {
                                '$set':{
                                    'Country_Georgia': 'Yes'
                                            
                                }
                            }
                            )
                    else:
                        db[colname].update_one(
                            {
                                '_id': _doc['_id']
                            },
                            {
                                '$set':{
                                    'Country_Georgia': 'No'
                                            
                                }
                            }
                            )

        event_types = [doc['civic_new']['event_type'] for doc in docs]
        event_types2 = [None] * len(docs)
        #create a list of event_types based on data pulled
        for index, doc in enumerate(docs):

            if doc['civic_new']['event_type'] == 'legalaction':
                if check_coup(doc):
                    event_types[index] = 'legalaction'
                    event_types2[index] = 'coup'
                elif check_defamation(doc):
                    event_types[index] = 'legalaction'
                    event_types2[index] = 'defamationcase'
                else:
                    event_types[index] = 'legalaction'
                    if check_double(doc):
                        event_types2[index] = 'corruption'

            elif doc['civic_new']['event_type'] == 'censor':
                if check_censorship(doc):
                    event_types[index] = 'censor'
                else: 
                    event_types[index] = '-999'
                    
            elif doc['civic_new']['event_type'] == 'arrest':
                if check_double(doc):
                    event_types[index] = 'arrest'
                    event_types2[index] = 'corruption'
                else:
                    event_types[index] = 'arrest'
            
            elif doc['civic_new']['event_type'] == 'purge':
                if check_double(doc):
                    event_types[index] = 'purge'
                    event_types2[index] = 'corruption'
                else:
                    event_types[index] = 'purge'
            
            elif doc['civic_new']['event_type'] == 'corruption':
                if check_corruption_PU(doc):
                    event_types[index] = 'corruption'
                    event_types2[index] = 'purge'
                elif check_corruption_LA(doc):
                    event_types[index] = 'corruption'
                    event_types2[index] = 'legalaction'
                elif check_corruption_AR(doc):
                    event_types[index] = 'corruption'
                    event_types2[index] = 'arrest'
                else:
                    event_types[index] = 'corruption'
       

            else:
                event_types[index] = doc['civic_new']['event_type']
                
        proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, event_types2 = event_types2, colname = colname))
        proc.start()

    
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    df.to_csv(path + f'{domain}.csv')

if __name__ == "__main__":
    slp = False
    # slp = True
    
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
        ('Kosovo', 'XKX'),/
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
        ('Solomon Islands', 'SLB')
    ]
    

    countries_needed = ['XKX','DOM','MDA','IDN','MKD','KHM','SLB','BLR','BGD']

    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]


    
    
# 'LKA','XKX','SRB','MDA','HUN','BLR','MYS','UZB','IND'
# 'CMR','NPL','GHA','TUN','SSD','PER','MAR','MOZ','IDN','AGO','PRY','UGA'
# 'PHL','AGO','MRT','MAR','UGA','TZA','SRB'
# 'COL','PRY','SEN','KGZ','HND','LBR','KEN'
# 'XKX','DOM','MDA','IDN','MKD','KHM','SLB','BLR','BGD'

    for ctup in countries:

        print('Starting: '+ctup[0])

        country_name = ctup[0]
        country_code = ctup[1]

        #loc=['elsalvador.com']
        if country_code == 'XKX':
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {
                    'primary_location': {'$in': [country_code]},
                    'include': True
                }
            )]+['balkaninsight.com']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True}) if doc['source_domain']!='balkaninsight.com']
        
        elif country_code == 'KAZ':
            loc = [doc['source_domain'] for doc in db['sources'].find(
                {
                    'primary_location': {'$in': [country_code]},
                    'include': True
                }
            )]+['kaztag.kz']
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]
            
        else:

            loc = [doc['source_domain'] for doc in db['sources'].find(
                {
                    'primary_location': {'$in': [country_code]},
                    'include': True
                }
            )]
            ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
            regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]

        # p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=8)
        # p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=8)
        # p_umap(count_domain_int, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=8)
        # loc = ['diarioelsalvador.com']
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

# screen -S screen_count
# screen -r screen_count
# conda activate peace

# cd /home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts
#'ETH','TZA','BEN','COL','ECU','DZA','NIC','KEN','JAM','GTM','MLI','SEN','ZWE','COD'

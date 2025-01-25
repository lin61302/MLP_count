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

censor_re = re.compile(r'((?<![a-zA-Z])freedom.*|(?<![a-zA-Z])assembl.*|(?<![a-zA-Z])associat.*|(?<![a-zA-Z])term limit.*|(?<![a-zA-Z])independen.*|(?<![a-zA-Z])succession.*|(?<![a-zA-Z])demonstrat.*|crackdown.*|(?<![a-zA-Z])draconian|(?<![a-zA-Z])censor.*|authoritarian|(?<![a-zA-Z])repress.*|(?<![a-zA-Z])NGO(?<![a-zA-Z])|(?<![a-zA-Z])human rights|(?<![a-zA-Z])journal.*|(?<![a-zA-Z])newspaper|(?<![a-zA-Z])media|(?<![a-zA-Z])outlet|(?<![a-zA-Z])censor|(?<![a-zA-Z])reporter|(?<![a-zA-Z])broadcast.*|(?<![a-zA-Z])correspondent|(?<![a-zA-Z])press|magazine|(?<![a-zA-Z])paper|(?<![a-zA-Z])black out|blacklist|(?<![a-zA-Z])suppress|(?<![a-zA-Z])speaking|(?<![a-zA-Z])false news|(?<![a-zA-Z])fake news|(?<![a-zA-Z])radio|(?<![a-zA-Z])commentator|(?<![a-zA-Z])blogger|pposition voice|(?<![a-zA-Z])voice of the opposition|(?<![a-zA-Z])speech|broadcast|(?<![a-zA-Z])publish)',flags=re.IGNORECASE)

defame_re1 = re.compile(r'((?<![a-zA-Z])case|(?<![a-zA-Z])lawsuit|(?<![a-zA-Z])sue|(?<![a-zA-Z])suing|(?<![a-zA-Z])suing|(?<![a-zA-Z])suit|(?<![a-zA-Z])trial|(?<![a-zA-Z])court|(?<![a-zA-Z])charge|(?<![a-zA-Z])charging|(?<![a-zA-Z])rule|(?<![a-zA-Z])ruling|(?<![a-zA-Z])sentence|(?<![a-zA-Z])sentencing|(?<![a-zA-Z])judg.*)',flags=re.IGNORECASE)
defame_re2 = re.compile(r'((?<![a-zA-Z])defamation|(?<![a-zA-Z])defame|(?<![a-zA-Z])defam|(?<![a-zA-Z])libel|slander|(?<![a-zA-Z])insult|reputation|(?<![a-zA-Z])lese majeste|(?<![a-zA-Z])lese majesty|(?<![a-zA-Z])lese-majeste)',flags=re.IGNORECASE)

double_re = re.compile(r'(embezzle|embezzled|embezzling|embezzlement|(?<![a-zA-Z])bribe|(?<![a-zA-Z])bribes|(?<![a-zA-Z])bribed|(?<![a-zA-Z])bribing|(?<![a-zA-Z])gift|(?<![a-zA-Z])gifts|(?<![a-zA-Z])fraud|(?<![a-zA-Z])fraudulent|(?<![a-zA-Z])corrupt|corruption|(?<![a-zA-Z])procure|(?<![a-zA-Z])procured|procurement|(?<![a-zA-Z])budget|(?<![a-zA-Z])assets|irregularities|(?<![a-zA-Z])graft|(?<![a-zA-Z])enrich|(?<![a-zA-Z])enriched|(?<![a-zA-Z])enrichment|laundering)',flags=re.IGNORECASE)

corrupt_LA_re = re.compile(r'(legal process|(?<![a-zA-Z])case|investigat.*|(?<![a-zA-Z])appeal|prosecut.*|(?<![a-zA-Z])appeal|lawsuit|(?<![a-zA-Z])sue|(?<![a-zA-Z])suing|(?<![a-zA-Z])trial|(?<![a-zA-Z])court|(?<![a-zA-Z])charg.*|(?<![a-zA-Z])rule|(?<![a-zA-Z])ruling|(?<![a-zA-Z])sentenc.*|(?<![a-zA-Z])judg.*)',flags=re.IGNORECASE)
corrupt_AR_re = re.compile(r'((?<![a-zA-Z])arrest|(?<![a-zA-Z])detain|(?<![a-zA-Z])apprehend|(?<![a-zA-Z])captur.*|(?<![a-zA-Z])custod.*|imprison|(?<![a-zA-Z])jail)',flags=re.IGNORECASE)
corrupt_PU_re = re.compile(r'((?<![a-zA-Z])resign|(?<![a-zA-Z])fire|(?<![a-zA-Z])firing|(?<![a-zA-Z])dismiss|(?<![a-zA-Z])sack|(?<![a-zA-Z])replac.*|(?<![a-zA-Z])quit)',flags=re.IGNORECASE)

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
        

        cur = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                '$or': [
                    {'cliff_locations.' + country_code : {'$exists' : True}},
                    {'cliff_locations' : {}}
                ]
            }
        )
        docs = [doc for doc in cur]

        for et in events:

            if et == 'legalaction':
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
                sub_docs = [doc for doc in sub_docs if check_defamation(doc)]
                
                
                
            
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
                sub_docs3 = [doc for doc in sub_docs3 if (check_double(doc)) and (not check_defamation(doc))]
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
                if check_defamation(doc):
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
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Roya_counts/{country_name}/Combined/'
    
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

        cur = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'cliff_locations.' + country_code : {'$exists' : True},
            }, batch_size=1
        )
        docs = [doc for doc in cur]

 
        for et in events:

            if et == 'legalaction':
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
                sub_docs = [doc for doc in sub_docs if check_defamation(doc)]
                
                
                
            
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
                sub_docs3 = [doc for doc in sub_docs3 if (check_double(doc)) and (not check_defamation(doc))]
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
                if check_defamation(doc):
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

    
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Roya_counts/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    df.to_csv(path + f'{domain}.csv')

if __name__ == "__main__":
    # t = 5400
    # print(f'start sleeping for {t/60} mins')
    # time.sleep(t)
    
    countries = [
        ('Albania', 'ALB'), 
        # ('Benin', 'BEN'),
        # ('Colombia', 'COL'),
        # ('Ecuador', 'ECU'),
        # ('Ethiopia', 'ETH'),
        ('Georgia', 'GEO'),
        # ('Kenya', 'KEN'),
        # ('Paraguay', 'PRY'),
        # ('Mali', 'MLI'),
        # ('Morocco', 'MAR'),
        # ('Nigeria', 'NGA'),
        ('Serbia', 'SRB'),
        # ('Senegal', 'SEN'),
        # ('Tanzania', 'TZA'),
        # ('Uganda', 'UGA'),
        ('Ukraine', 'UKR'),
        # ('Zimbabwe', 'ZWE'),
        # ('Mauritania', 'MRT'),
        # ('Zambia', 'ZMB'),
        ('Kosovo', 'XKX'),
        # ('Niger', 'NER'),
        # ('Jamaica', 'JAM'),
        # ('Honduras', 'HND'),
        # ('Philippines', 'PHL'),
        # ('Ghana', 'GHA'),
        # ('Rwanda','RWA'),
        # ('Guatemala','GTM'),
        ('Belarus','BLR'),
        # ('Cambodia','KHM'),
        # ('DR Congo','COD'),
        ('Turkey','TUR'),
        # ('Bangladesh', 'BGD'),
        # ('El Salvador', 'SLV'),
        # ('South Africa', 'ZAF'),
        # ('Tunisia','TUN'),
        # ('Indonesia','IDN'),
        # ('Nicaragua','NIC'),
        # ('Angola','AGO'),
        ('Armenia','ARM'),
        # ('Sri Lanka', 'LKA'),
        # ('Malaysia','MYS'),
        # ('Cameroon','CMR'),
        ('Hungary','HUN'),
        # ('Malawi','MWI'),
        ('Uzbekistan','UZB'),
        # ('India','IND'),
        # ('Mozambique','MOZ'),
        ('Azerbaijan','AZE'),
        ('Kyrgyzstan','KGZ'),
        ('Moldova','MDA'),
        ('Kazakhstan','KAZ'),
        # ('Peru','PER')
        # ('Algeria','DZA')
        # ('Macedonia','MKD')
        # ('South Sudan','SSD')

    ]

    


    # Zambia, Kosovo, Mauritania, Bangladesh, Bolivia, Bosnia, Cambodia, CAR, DRC, El Salvador, Ghana, Guatemala, Honduras, Hungary, India, Indonesia, Iraq, Jamaica, Jordan, Kazakhstan, Liberia, Libya, Malawi, Malaysia, Mexico, Mongolia, Mozambique, Myanmar, Nepal, Nicaragua, Niger, Pakistan, Philippines, Rwanda, South Africa, South Sudan, Thailand, Yemen

    for ctup in countries:

        print('Starting: '+ctup[0])

        country_name = ctup[0]
        country_code = ctup[1]

        #loc=['elsalvador.com']
        if country_code == 'ALB':
            loc = ['lajme.rtsh.al', 'top-channel.tv']
        elif country_code == 'AZE':
            loc = ['azerbaijan-news.az', 'azertag.az']
        elif country_code == 'ARM':
            loc = ['1lurer.am', 'en.armradio.am','hhpress.am']
        elif country_code == 'BLR':
            loc = ['eng.belta.by', 'sb.by']
        elif country_code == 'GEO':
            loc = ['agenda.ge']
        elif country_code == 'HUN':
            loc = ['magyarnemzet.hu']
        elif country_code == 'KAZ':
            loc = ['kazpravda.kz', 'qazaqstan.tv']
        elif country_code == 'XKX':
            loc = ['rtklive.com']
        elif country_code == 'KGZ':
            loc = ['ktrk.kg']
        elif country_code == 'MDA':
            loc = ['moldova-suverana.md']
        elif country_code == 'SRB':
            loc = ['rts.rs']
        elif country_code == 'TUR':
            loc = ['trthaber.com']
        elif country_code == 'UKR':
            loc = ['ukrinform.net']
        elif country_code == 'UZB':
            loc = ['uza.uz']
        

        # p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=8)
        # p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=8)
        # p_umap(count_domain_int, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=8)
        # loc = ['diarioelsalvador.com']
        ind = 1
        while ind:
            try:
                p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)
                ind = 0
            except:
                pass
            
        # ind = 1
        # while ind:
        #     try:
        #         p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=10)
        #         ind = 0
        #     except:
        #         pass
        # ind = 1
        # while ind:
        #     try:
        #         p_umap(count_domain_int, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=10)
        #         ind = 0
        #     except:
        #         pass

# screen -S screen_count
# screen -r screen_count
# conda activate peace

# cd /home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts


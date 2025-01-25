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

today = pd.Timestamp.now()

events = [k for k in db.models.find_one({'model_name': 'civic_new'}).get('event_type_nums').keys()] + ['defamationcase'] +['total_articles']

# legal_re = re.compile(r'((?<![a-z])freedom.*|(?<![a-z])assembl.*|(?<![a-z])associat.*|(?<![a-z])term limit.*|(?<![a-z])independen.*|(?<![a-z])succession.*|(?<![a-z])demonstrat.*|(?<![a-z])crackdown.*|(?<![a-z])draconian|(?<![a-z])censor.*|(?<![a-z])authoritarian|(?<![a-z])repress.*|(?<![a-z])NGO(?![a-z])|(?<![a-z])human rights)',flags=re.IGNORECASE)

censor_re = re.compile(r'((?<![a-z])freedom.*|(?<![a-z])assembl.*|(?<![a-z])associat.*|(?<![a-z])term limit.*|(?<![a-z])independen.*|(?<![a-z])succession.*|(?<![a-z])demonstrat.*|crackdown.*|(?<![a-z])draconian|(?<![a-z])censor.*|authoritarian|(?<![a-z])repress.*|(?<![a-z])NGO(?![a-z])|(?<![a-z])human rights|(?<![a-z])journal.*|(?<![a-z])newspaper|(?<![a-z])media|(?<![a-z])outlet|(?<![a-z])censor|(?<![a-z])reporter|(?<![a-z])broadcast.*|(?<![a-z])correspondent|(?<![a-z])press|magazine|(?<![a-z])paper|(?<![a-z])black out|blacklist|(?<![a-z])suppress|(?<![a-z])speaking|(?<![a-z])false news|(?<![a-z])fake news|(?<![a-z])radio|(?<![a-z])commentator|(?<![a-z])blogger|pposition voice|(?<![a-z])voice of the opposition|(?<![a-z])speech|broadcast|(?<![a-z])publish)',flags=re.IGNORECASE)

defame_re1 = re.compile(r'((?<![a-z])case|(?<![a-z])lawsuit|(?<![a-z])sue|(?<![a-z])suing|(?<![a-z])suing|(?<![a-z])suit|(?<![a-z])trial|(?<![a-z])court|(?<![a-z])charge|(?<![a-z])charging|(?<![a-z])rule|(?<![a-z])ruling|(?<![a-z])sentence|(?<![a-z])sentencing|(?<![a-z])judg.*)',flags=re.IGNORECASE)
defame_re2 = re.compile(r'((?<![a-z])defamation|(?<![a-z])defame|(?<![a-z])defam|(?<![a-z])libel|slander|(?<![a-z])insult|reputation|(?<![a-z])lese majeste|(?<![a-z])lese majesty|(?<![a-z])lese-majeste)',flags=re.IGNORECASE)

double_re = re.compile(r'(embezzle|embezzled|embezzling|embezzlement|(?<![a-z])bribe|(?<![a-z])bribes|(?<![a-z])bribed|(?<![a-z])bribing|(?<![a-z])gift|(?<![a-z])gifts|(?<![a-z])fraud|(?<![a-z])fraudulent|(?<![a-z])corrupt|corruption|(?<![a-z])procure|(?<![a-z])procured|procurement|(?<![a-z])budget|(?<![a-z])assets|irregularities|(?<![a-z])graft|(?<![a-z])enrich|(?<![a-z])enriched|(?<![a-z])enrichment|laundering)',flags=re.IGNORECASE)

corrupt_LA_re = re.compile(r'(legal process|(?<![a-z])case|investigat.*|(?<![a-z])appeal|prosecut.*|(?<![a-z])appeal|lawsuit|(?<![a-z])sue|(?<![a-z])suing|(?<![a-z])trial|(?<![a-z])court|(?<![a-z])charg.*|(?<![a-z])rule|(?<![a-z])ruling|(?<![a-z])sentenc.*|(?<![a-z])judg.*)',flags=re.IGNORECASE)
corrupt_AR_re = re.compile(r'((?<![a-z])arrest|(?<![a-z])detain|(?<![a-z])apprehend|(?<![a-z])captur.*|(?<![a-z])custod.*|imprison|(?<![a-z])jail)',flags=re.IGNORECASE)
corrupt_PU_re = re.compile(r'((?<![a-z])resign|(?<![a-z])fire|(?<![a-z])firing|(?<![a-z])dismiss|(?<![a-z])sack|(?<![a-z])replac.*|(?<![a-z])quit)',flags=re.IGNORECASE)

__georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
__georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

geo_int = pd.read_excel(__georgiapath_int__)
geo_loc = pd.read_excel(__georgiapath_loc__)
g_int = geo_int['CompanyName'].str.strip()
g_loc = geo_loc['CompanyName'].str.strip()
for i, doc in enumerate(g_int):
    g_int[i] = "(?<![a-z])" + g_int[i][2:-2].rstrip().lstrip() + "(?![a-z])"
for i, doc in enumerate(g_loc):
    g_loc[i] = "(?<![a-z])" + g_loc[i][2:-2].rstrip().lstrip() + "(?![a-z])"
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
        if bool(bool(defame_re1.search(doc.get('title_translated'))) or bool(defame_re1.search(doc.get('maintext_translated')))) and bool(bool(defame_re2.search(doc.get('title_translated'))) or bool(defame_re2.search(doc.get('maintext_translated')))):
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
    

def update_info(docs, event_types, colname):
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
                    'event_type_civic_new':event_types[nn]
                            
                }
            }
        )

# START WITH THE LOCALS
def count_domain_loc(uri, domain, country_name, country_code):

    db = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today+ pd.Timedelta(1, 'd')   , freq='D')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]
    df['day'] = [dd.day for dd in df.index]

    prev_colname = ''


    

    for et in events:
        df[et] = [0] * len(df)

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        if colname == prev_colname:
            pass

        else:
            prev_colname = colname
            
            

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
            month_docs = [doc for doc in cur]

        docs = []
        for doc in month_docs:
            try:
                if doc['date_publish'].month == date.month and doc['date_publish'].day == date.day and doc['date_publish'].year == date.year:
                    docs.append(doc)
                else:
                    pass
            except:
                pass


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
                count = len(sub_docs)


            else:
                count = len(sub_docs)


            df.loc[date, et] = count
        

            
            
        # if country_code == 'GEO':
        #     for nn, _doc in enumerate(docs):
        #             try:
        #                 colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        #             except:
        #                 dd = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
        #                 colname = f"articles-{dd.year}-{dd.month}"
        #             if check_georgia(_doc['maintext_translated'], _domain='loc') and check_georgia(_doc['title_translated'], _domain='loc'):
        #                 db[colname].update_one(
        #                     {
        #                         '_id': _doc['_id']
        #                     },
        #                     {
        #                         '$set':{
        #                             'Country_Georgia': 'Yes'
                                            
        #                         }
        #                     }
        #                     )
        #             else:
        #                 db[colname].update_one(
        #                     {
        #                         '_id': _doc['_id']
        #                     },
        #                     {
        #                         '$set':{
        #                             'Country_Georgia': 'No'
                                            
        #                         }
        #                     }
        #                     )


        #original event type
        # event_types = [doc['civic1']['event_type'] for doc in docs]
        #update event type based on filter outcome
        # for index, doc in enumerate(docs):

        #     if doc['civic1']['event_type'] == 'legalaction' and check_defamation(doc):
        #         event_types[index] = 'defamationcase'

        #     elif doc['civic1']['event_type'] == 'legalchange':
        #         if check_legal(doc):
        #             event_types[index] = 'legalchange'
        #         else: 
        #             event_types[index] = '-999'

        #     elif doc['civic1']['event_type'] == 'censor':
        #         if check_censorship(doc):
        #             event_types[index] = 'censor'
        #         else: 
        #             event_types[index] = '-999'

        #     else:
        #         event_types[index] = doc['civic1']['event_type']

        

        # #update data with new event_types
        # proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, colname = colname))
        # proc.start()

            




    # check if directory exists
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_Daily/2023-5-civic_new/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)
    df.to_csv(path + f'{domain}.csv')


# Then ints
def count_domain_int(uri, domain, country_name, country_code):

    db = MongoClient(uri).ml4p

    df = pd.DataFrame()
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today+ pd.Timedelta(1, 'd')   , freq='D')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]
    df['day'] = [dd.day for dd in df.index]

    prev_colname = ''
    

    for et in events:
        df[et] = [0] * len(df)
    
    

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        if colname == prev_colname:
            pass

        else:
            prev_colname = colname


            cur = db[colname].find(
                {
                    'source_domain': domain,
                    'include': True,
                    'civic_new': {'$exists': True},
                    'cliff_locations.' + country_code : {'$exists' : True},
                }
            )
            month_docs = [doc for doc in cur]



        docs = []
        for doc in month_docs:
            try:
                if doc['date_publish'].month == date.month and doc['date_publish'].day == date.day and doc['date_publish'].year == date.year:
                    docs.append(doc)
                else:
                    pass
            except:
                pass
    
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
            count = len(sub_docs)

            df.loc[date, et] = count
                        

        # if country_code == 'GEO':
        #     for nn, _doc in enumerate(docs):
        #             try:
        #                 colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        #             except:
        #                 dd = dateparser.parse(_doc['date_publish']).replace(tzinfo = None)
        #                 colname = f"articles-{dd.year}-{dd.month}"
        #             if check_georgia(_doc['maintext_translated'], _domain='int') and check_georgia(_doc['title_translated'], _domain='int'):
        #                 db[colname].update_one(
        #                     {
        #                         '_id': _doc['_id']
        #                     },
        #                     {
        #                         '$set':{
        #                             'Country_Georgia': 'Yes'
                                            
        #                         }
        #                     }
        #                     )
        #             else:
        #                 db[colname].update_one(
        #                     {
        #                         '_id': _doc['_id']
        #                     },
        #                     {
        #                         '$set':{
        #                             'Country_Georgia': 'No'
                                            
        #                         }
        #                     }
        #                     )

        # event_types = [doc['civic1']['event_type'] for doc in docs]
        #create a list of event_types based on data pulled
        # for index, doc in enumerate(docs):

        #     if doc['civic1']['event_type'] == 'legalaction' and check_defamation(doc):
        #         event_types[index] = 'defamationcase'

        #     elif doc['civic1']['event_type'] == 'legalchange':
        #         if check_legal(doc):
        #             event_types[index] = 'legalchange'
        #         else: 
        #             event_types[index] = '-999'

        #     elif doc['civic1']['event_type'] == 'censor':
        #         if check_censorship(doc):
        #             event_types[index] = 'censor'
        #         else: 
        #             event_types[index] = '-999'
        #     else:
        #         event_types[index] = doc['civic1']['event_type']

        # proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, colname = colname))
        # proc.start()

    
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_Daily/2023-5-civic_new/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    df.to_csv(path + f'{domain}.csv')

if __name__ == "__main__":
    
    countries = [
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
        ('Hungary','HUN')
        ('Malawi','MWI'),
        ('Uzbekistan','UZB'),
        ('India','IND'),
        ('Mozambique','MOZ'),
        ('Azerbaijan','AZE'),
        ('Kyrgyzstan','KGZ'),
        ('Moldova','MDA'),
        ('Kazakhstan','KAZ'),
        ('Peru','PER')
    ]


    # Zambia, Kosovo, Mauritania, Bangladesh, Bolivia, Bosnia, Cambodia, CAR, DRC, El Salvador, Ghana, Guatemala, Honduras, Hungary, India, Indonesia, Iraq, Jamaica, Jordan, Kazakhstan, Liberia, Libya, Malawi, Malaysia, Mexico, Mongolia, Mozambique, Myanmar, Nepal, Nicaragua, Niger, Pakistan, Philippines, Rwanda, South Africa, South Sudan, Thailand, Yemen

    for ctup in countries:

        print('Starting: '+ctup[0])

        country_name = ctup[0]
        country_code = ctup[1]

        #loc=['elsalvador.com']

        loc = [doc['source_domain'] for doc in db['sources'].find(
            {
                'primary_location': {'$in': [country_code]},
                'include': True
            }
        )]
        ints = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
        regionals = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]

        p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=8)
        p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=8)
        p_umap(count_domain_int, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=8)

# screen -S screen_count
# screen -r screen_count
# conda activate peace

# cd /home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts


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
uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
db = MongoClient(uri).ml4p

today = pd.Timestamp.now()

events = [k for k in db.models.find_one({'model_name': 'civic1'}).get('event_type_nums').keys()] + ['-999']

# legal_re = re.compile(r'((?<![a-z])freedom.*|(?<![a-z])assembl.*|(?<![a-z])associat.*|(?<![a-z])term limit.*|(?<![a-z])independen.*|(?<![a-z])succession.*|(?<![a-z])demonstrat.*|(?<![a-z])crackdown.*|(?<![a-z])draconian|(?<![a-z])censor.*|(?<![a-z])authoritarian|(?<![a-z])repress.*|(?<![a-z])NGO(?![a-z])|(?<![a-z])human rights)',flags=re.IGNORECASE)

censor_re = re.compile(r'((?<![a-z])freedom.*|(?<![a-z])assembl.*|(?<![a-z])associat.*|(?<![a-z])term limit.*|(?<![a-z])independen.*|(?<![a-z])succession.*|(?<![a-z])demonstrat.*|(?<![a-z])crackdown.*|(?<![a-z])draconian|(?<![a-z])censor.*|(?<![a-z])authoritarian|(?<![a-z])repress.*|(?<![a-z])NGO(?![a-z])|(?<![a-z])human rights|(?<![a-z])journal.*|(?<![a-z])newspaper|(?<![a-z])media|(?<![a-z])outlet|(?<![a-z])censor|(?<![a-z])reporter|(?<![a-z])broadcast.*|(?<![a-z])correspondent|(?<![a-z])press|(?<![a-z])magazine|(?<![a-z])paper|(?<![a-z])black out|(?<![a-z])blacklist|(?<![a-z])suppress|(?<![a-z])speaking|(?<![a-z])false news|(?<![a-z])fake news|(?<![a-z])radio|(?<![a-z])commentator|(?<![a-z])blogger|(?<![a-z])pposition voice|(?<![a-z])voice of the opposition|(?<![a-z])speech|(?<![a-z])broadcast|(?<![a-z])publish)',flags=re.IGNORECASE)

defame_re1 = re.compile(r'((?<![a-z])case|(?<![a-z])lawsuit|(?<![a-z])sue|(?<![a-z])suit|(?<![a-z])trial)',flags=re.IGNORECASE)
defame_re2 = re.compile(r'((?<![a-z])defamation|(?<![a-z])defame|(?<![a-z])libel|(?<![a-z])slander|(?<![a-z])insult|(?<![a-z])reputation|(?<![a-z])lese majeste|(?<![a-z])lese majesty|(?<![a-z])lese-majeste)',flags=re.IGNORECASE)

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


def check_legal(doc):
    try:
        if bool(legal_re.search(doc.get('title_translated'))) or bool(legal_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False

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
                    'event_type_civic1':event_types[nn]
                            
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
                'civic1': {'$exists': True},
                '$or': [
                    {'cliff_locations.' + country_code : {'$exists' : True}},
                    {'cliff_locations' : {}}
                ]
            }
        )
        docs = [doc for doc in cur]

        for et in events:

            if et == 'legalaction':
                #sub_docs = [doc for doc in docs if doc['civic1']['event_type'] == 'legalaction']
                sub_docs = []
                
                for doc in docs:
                    try:
                        if doc['civic1']['event_type'] == et:
                        
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if not check_defamation(doc)]
            
            if et == 'defamationcase':
                #sub_docs = [doc for doc in docs if doc['civic1']['event_type'] == 'legalaction']
                sub_docs = []
                
                for doc in docs:
                    try:
                        if doc['civic1']['event_type'] == et or doc['civic1']['event_type'] == 'legalaction':
                        
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if check_defamation(doc)]
                

            # elif et == 'legalchange':
            #     #sub_docs = [doc for doc in docs if doc['civic1']['event_type'] == 'legalchange']
            #     sub_docs = []
            #     for doc in docs:
            #         try:
            #             if doc['civic1']['event_type'] == et:
                        
            #                 sub_docs.append(doc)
            #         except:
            #             print(doc)
            #     sub_docs = [doc for doc in sub_docs if check_legal(doc)]
                
            
            elif et == 'censor':
                #sub_docs = [doc for doc in docs if doc['civic1']['event_type'] == 'censor']
                sub_docs = []
                for doc in docs:
                    try:
                        if doc['civic1']['event_type'] == et:
                        
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if check_censorship(doc)]
                
            
            

            else: 
                sub_docs = []
                for doc in docs:
                    try:
                        if doc['civic1']['event_type'] == et:
                        
                            sub_docs.append(doc)
                    except:
                        print(doc)

            if country_code == 'GEO':
                sub_docs = [doc for doc in sub_docs if check_georgia(doc['maintext_translated'], _domain='loc') and check_georgia(doc['title_translated'], _domain='loc')]
                count = len(sub_docs)


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
        event_types = [doc['civic1']['event_type'] for doc in docs]
        #update event type based on filter outcome
        for index, doc in enumerate(docs):

            if doc['civic1']['event_type'] == 'legalaction':
                if check_defamation(doc):
                    event_types[index] = 'defamationcase'
                else:
                    event_types[index] = 'legalaction'
                
            elif doc['civic1']['event_type'] == 'defamationcase': 
                if check_defamation(doc):
                    event_types[index] = 'defamationcase'
                else:
                    event_types[index] = '-999'

            # elif doc['civic1']['event_type'] == 'legalchange':
            #     if check_legal(doc):
            #         event_types[index] = 'legalchange'
            #     else: 
            #         event_types[index] = '-999'

            elif doc['civic1']['event_type'] == 'censor':
                if check_censorship(doc):
                    event_types[index] = 'censor'
                else: 
                    event_types[index] = '-999'

            else:
                event_types[index] = doc['civic1']['event_type']

        

        #update data with new event_types
        proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, colname = colname))
        proc.start()

            




    # check if directory exists
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic/{country_name}/{today.year}_{today.month}_{today.day}/'
    
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
                'civic1': {'$exists': True},
                'cliff_locations.' + country_code : {'$exists' : True},
            }
        )
        docs = [doc for doc in cur]

 
        for et in events:

            if et == 'legalaction':
                #sub_docs = [doc for doc in docs if doc['civic1']['event_type'] == 'legalaction']
                sub_docs = []
                
                for doc in docs:
                    try:
                        if doc['civic1']['event_type'] == et:
                        
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if not check_defamation(doc)]
            
            if et == 'defamationcase':
                #sub_docs = [doc for doc in docs if doc['civic1']['event_type'] == 'legalaction']
                sub_docs = []
                
                for doc in docs:
                    try:
                        if doc['civic1']['event_type'] == et or doc['civic1']['event_type'] == 'legalaction':
                        
                            sub_docs.append(doc)
                    except:
                        print(doc)
                sub_docs = [doc for doc in sub_docs if check_defamation(doc)]


            # elif et == 'legalchange':
            #     #sub_docs = [doc for doc in docs if doc['civic1']['event_type'] == 'legalchange']
            #     sub_docs = []
            #     for doc in docs:
            #         try:
            #             if doc['civic1']['event_type'] == et:
                        
            #                 sub_docs.append(doc)
            #         except:
            #             pass
            #     sub_docs = [doc for doc in sub_docs if check_legal(doc)]

            
            elif et == 'censor':
                #sub_docs = [doc for doc in docs if doc['civic1']['event_type'] == 'censor']
                sub_docs = []
                for doc in docs:
                    try:
                        if doc['civic1']['event_type'] == et:
                        
                            sub_docs.append(doc)
                    except:
                        pass
                sub_docs = [doc for doc in sub_docs if check_censorship(doc)]
             

            else: 
                
                sub_docs = []
                for doc in docs:
                    try:
                        if doc['civic1']['event_type'] == et:
                        
                            sub_docs.append(doc)
                    except:
                        pass

                
            if country_code == 'GEO':
                sub_docs = [doc for doc in sub_docs if check_georgia(doc['maintext_translated'], _domain='int') and check_georgia(doc['title_translated'], _domain='int')]
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

        event_types = [doc['civic1']['event_type'] for doc in docs]
        #create a list of event_types based on data pulled
        for index, doc in enumerate(docs):

            if doc['civic1']['event_type'] == 'legalaction':
                if check_defamation(doc):
                    event_types[index] = 'defamationcase'
                else:
                    event_types[index] = 'legalaction'
                
            elif doc['civic1']['event_type'] == 'defamationcase': 
                if check_defamation(doc):
                    event_types[index] = 'defamationcase'
                else:
                    event_types[index] = '-999'

            # elif doc['civic1']['event_type'] == 'legalchange':
            #     if check_legal(doc):
            #         event_types[index] = 'legalchange'
            #     else: 
            #         event_types[index] = '-999'

            elif doc['civic1']['event_type'] == 'censor':
                if check_censorship(doc):
                    event_types[index] = 'censor'
                else: 
                    event_types[index] = '-999'
            else:
                event_types[index] = doc['civic1']['event_type']

        proc = multiprocessing.Process(target=update_info(docs = docs, event_types = event_types, colname = colname))
        proc.start()

    
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic/{country_name}/{today.year}_{today.month}_{today.day}/'
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    df.to_csv(path + f'{domain}.csv')

if __name__ == "__main__":
    
    countries = [
        # ('Albania', 'ALB'), 
        # ('Benin', 'BEN'),
        # ('Colombia', 'COL'),
        # ('Ecuador', 'ECU'),
        # ('Ethiopia', 'ETH'),
        # ('Georgia', 'GEO'),
        # ('Kenya', 'KEN'),
        # ('Paraguay', 'PRY'),
        # ('Mali', 'MLI'),
        # ('Morocco', 'MAR'),
        # ('Nigeria', 'NGA'),
        # ('Serbia', 'SRB'),
        # ('Senegal', 'SEN'),
        # ('Tanzania', 'TZA'),
        # ('Uganda', 'UGA'),
        # ('Ukraine', 'UKR'),
        # ('Zimbabwe', 'ZWE'),
        ('Mauritania', 'MRT'),
        # ('Zambia', 'ZMB'),
        # ('Kosovo', 'XKX'),
        # ('Niger', 'NER'),
        # ('Jamaica', 'JAM'),
        # ('Honduras', 'HND'),
        # ('Philippines', 'PHL'),
        # ('Ghana', 'GHA'),
        # ('Rwanda','RWA'),
        # ('Guatemala','GTM'),
        # ('Ecuador', 'ECU'),
        # ('Belarus','BLR'),
        # ('Congo','COD'),
        # ('Cambodia','KHM'),
        # ('Turkey','TUR')
        # ('Bangladesh', 'BGD'),
        # ('El Salvador', 'SLV'),
        # ('South Africa', 'ZAF')
        # ('Tunisia','TUN')
        # ('Indonesia','IDN')
        # ('Nicaragua','NIC'),
        # ('Angola','AGO'),
        # ('Armenia','ARM'),
        # ('Sri Lanka', 'LKA')
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


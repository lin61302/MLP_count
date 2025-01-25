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
today = pd.Timestamp.now() - pd.Timedelta(days=1)

events = ['economic_crisis', 'total_articles']

# legal_re = re.compile(r'((?<![a-zA-Z])freedom.*|(?<![a-zA-Z])assembl.*|(?<![a-zA-Z])associat.*|(?<![a-zA-Z])term limit.*|(?<![a-zA-Z])independen.*|(?<![a-zA-Z])succession.*|(?<![a-zA-Z])demonstrat.*|(?<![a-zA-Z])crackdown.*|(?<![a-zA-Z])draconian|(?<![a-zA-Z])censor.*|(?<![a-zA-Z])authoritarian|(?<![a-zA-Z])repress.*|(?<![a-zA-Z])NGO(?<![a-zA-Z])|(?<![a-zA-Z])human rights)',flags=re.IGNORECASE)

econ_re = re.compile(r'(economic cris(is|es)|(?<![a-zA-Z])recession|(?<![a-zA-Z])market crash|financial bailout|economic stimulus|economic stimuli|inflation, austerit.*|financial cris(is|es)|economic downturn|economic collaps.*|fiscal cris(is|es)|banking cris(is|es)|economic decline|economic instabilit.*|hyperinflation|balance of payments crisis|exchange rate cris(is|es)|negative growth|liquidity cris(is|es)|mortgage cris(is|es)|debt cris(is|es)|financial collapse|fiscal collapse|credit crunch|credit downgrade|stagflation|foreclosure rate|real estate bubble|financial meltdown|market turmoil|economic slowdown|investment slump|fiscal contraction|economic instabilit.*|financial distress|market volatilit.*|fiscal stimulus|fiscal stimuli|interest rate hike)',flags=re.IGNORECASE)
exclude_re = re.compile(r'(2008 financial|2008 economic|(?<![a-zA-Z])sports(?<![a-zA-Z])|football(?<![a-zA-Z])|(?<![a-zA-Z])club(?<![a-zA-Z])|(?<![a-zA-Z])team(?<![a-zA-Z]))',flags=re.IGNORECASE)
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

def check_econ(doc):
    try:
        if bool(econ_re.search(doc.get('title_translated'))) or bool(econ_re.search(doc.get('maintext_translated'))):
            return True
        else:
            return False
    except:
        return False

def exclude_econ(doc):
    try:
        if bool(exclude_re.search(doc.get('title_translated'))) or bool(exclude_re.search(doc.get('maintext_translated'))):
            return False
        else:
            return True
    except:
        return True


    
    

    

def update_info(docs_yes, docs_no, colname):
    """
    updates the docs into the db
    """
    db = MongoClient(uri).ml4p

    for nn, _doc in enumerate(docs_yes):
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
                    'econ_crisis':'Yes',
                   
                            
                }
            }
        )
    for nn, _doc in enumerate(docs_no):
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
                    'econ_crisis':'No',
                   
                            
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

            if et == 'economic_crisis':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'legalaction']
                sub_docs_yes = []
                sub_docs_no = []
                # sub_docs2 = []
                for doc in docs:
                    try:
                        if check_econ(doc) and exclude_econ(doc):
                            sub_docs_yes.append(doc)
                        else:
                            sub_docs_no.append(doc)
                    except:
                        sub_docs_no.append(doc)
                
                count = len(sub_docs_yes)


            
            elif et == 'total_articles':
                count = len(docs)

            

            df.loc[date, et] = count
        

        #update data with new event_types
        proc = multiprocessing.Process(target=update_info(docs_yes = sub_docs_yes, docs_no = sub_docs_no, colname = colname))
        proc.start()

            




    # check if directory exists
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Econ_Crisis/{country_name}/{today.year}_{today.month}_{today.day}/'
    
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

        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': {'$ne': 'en'},
                'cliff_locations.' + country_code : {'$exists' : True}, 
            }, batch_size=1
        )

        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_new': {'$exists': True},
                'language': 'en'  ,
                'en_cliff_locations.' + country_code : {'$exists' : True},
            }, batch_size=1
        )
        docs1 = [doc for doc in cur1]
        docs2 = [doc for doc in cur2]
        docs = docs1+docs2

 
        for et in events:

            if et == 'economic_crisis':
                #sub_docs = [doc for doc in docs if doc['civic_new']['event_type'] == 'legalaction']
                sub_docs_yes = []
                sub_docs_no = []
                # sub_docs2 = []
                for doc in docs:
                    try:
                        if check_econ(doc) and exclude_econ(doc):
                            sub_docs_yes.append(doc)
                        else:
                            sub_docs_no.append(doc)
                    except:
                        sub_docs_no.append(doc)
                
                count = len(sub_docs_yes)


            
            elif et == 'total_articles':
                count = len(docs)

            

            df.loc[date, et] = count
        

        #update data with new event_types
        proc = multiprocessing.Process(target=update_info(docs_yes = sub_docs_yes, docs_no = sub_docs_no, colname = colname))
        proc.start()

    
    path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Econ_Crisis/{country_name}/{today.year}_{today.month}_{today.day}/'
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    df.to_csv(path + f'{domain}.csv')

if __name__ == "__main__":
    slp = False
    # slp = True
    
    if slp:
        t = 10800
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
        ('Solomon Islands', 'SLB')
    ]
    

    countries_needed = ['XKX','DOM','MDA','IDN','MKD','KHM','SLB','BLR','BGD']

    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]

                

    # "Nigeria", "South Sudan", "Peru", "Tunisia", "Mauritania", "Cameroon",  "Ghana", "Morocco", "Mozambique", "Niger", "Paraguay", "Rwanda", "Uganda", "Angola", "Pakistan", "Colombia", "Ecuador", "Guatemala", "Jamaica", "Nicaragua", "Algeria", "Mali", "Benin", "Burkina Faso", "DR Congo", "Ethiopia", "Kenya", "Senegal", "Tanzania", "Zimbabwe"
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
            except:
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

# screen -S screen_count
# screen -r screen_count
# conda activate peace

# cd /home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts
# 'KEN', 'MLI', 'NIC', 'SEN', 'AZE', 'NGA', 'SSD', 'PER'

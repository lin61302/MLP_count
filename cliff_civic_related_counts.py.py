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
#uri = os.getenv('DATABASE_URL')
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

today = pd.Timestamp.now()
# today = pd.Timestamp('2025-07-22')
ind_dic = {'Yes':'Civic_related','No':'Non_Civic_Realed'}
events = [k for k in db.models.find_one({'model_name': 'civic_new'}).get('event_type_nums').keys()] + ['defamationcase']

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
    
    df2 = df.copy()

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"
        
        # cur = db[colname].find(
        #         {
        #             'source_domain': domain,
        #             'include': True,
        #             'civic_related': {'$exists': True},
        #             'event_type_civic_new': {'$exists': True},
        #             '$or': [
        #                 {'cliff_locations.' + country_code : {'$exists' : True}},
        #                 {'cliff_locations' : {}}
        #             ]
        #         }
        #     )

        # docs = [doc for doc in cur]
        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'language': {'$ne': 'en'},
                'civic_related': {'$exists': True},
                'event_type_civic_new': {'$exists': True},
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
                'language': 'en'  ,
                'civic_related': {'$exists': True},
                'event_type_civic_new': {'$exists': True},
                '$or': [
                    {'en_cliff_locations.' + country_code : {'$exists' : True}},
                    {'en_cliff_locations' : {}}
                ]
            }, batch_size=1
        )

        docs2 = [doc for doc in cur2]
        docs = docs1+docs2

    

        for et in events:
            sub_docs = []
            sub_docs2 = []

            if country_code == 'GEO':
                docs2 = docs
                docs = []
                for doc in docs2:
                    try:
                        if doc['Country_Georgia'] == 'Yes':
                            docs.append(doc)
                        else:
                            pass
                    except:
                        pass


            for doc in docs:
                try:
                    if doc['event_type_civic_new'] == et or doc['event_type_civic_new_2'] == et:

                        if doc['civic_related']['result'] == 'Yes':
                            sub_docs.append(doc)

                        elif doc['civic_related']['result'] == 'No':
                            sub_docs2.append(doc)
                    
                except Exception as err:
                    print(err)


            count = len(sub_docs)
            df.loc[date, et] = count

            count2 = len(sub_docs2)
            df2.loc[date, et] = count2

            


        
    path1 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Civic_Related/'
    path2 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Non_Civic_Related/'
    
    if not os.path.exists(path1):
        Path(path1).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(path2):
        Path(path2).mkdir(parents=True, exist_ok=True)
    df.to_csv(path1 + f'{domain}.csv')
    df2.to_csv(path2 + f'{domain}.csv')

def count_domain_int(uri, domain, country_name, country_code):

    db = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]

    

    for et in events:
        df[et] = [0] * len(df)
    
    df2 = df.copy()

    for date in df.index:
        colname = f"articles-{date.year}-{date.month}"

        # cur = db[colname].find(
        #         {
        #             'source_domain': domain,
        #             'include': True,
        #             'civic_related': {'$exists': True},
        #             'event_type_civic_new': {'$exists': True},
        #             'cliff_locations.' + country_code : {'$exists' : True}
        #         }
        #     )
        # docs = [doc for doc in cur]

        cur1 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_related': {'$exists': True},
                'language': {'$ne': 'en'},
                'cliff_locations.' + country_code : {'$exists' : True}, 
            }, batch_size=1
        )

        cur2 = db[colname].find(
            {
                'source_domain': domain,
                'include': True,
                'civic_related': {'$exists': True},
                'language': 'en'  ,
                'en_cliff_locations.' + country_code : {'$exists' : True},
            }, batch_size=1
        )
        docs1 = [doc for doc in cur1]
        docs2 = [doc for doc in cur2]
        docs = docs1+docs2

    

        for et in events:
            sub_docs = []
            sub_docs2 = []

            if country_code == 'GEO':
                docs2 = docs
                docs = []
                for doc in docs2:
                    try:
                        if doc['Country_Georgia'] == 'Yes':
                            docs.append(doc)
                        else:
                            pass
                    except:
                        pass

            for doc in docs:
                try:
                    
                    if doc['event_type_civic_new'] == et or doc['event_type_civic_new_2'] == et:

                        if doc['civic_related']['result'] == 'Yes':
                            sub_docs.append(doc)

                        elif doc['civic_related']['result'] == 'No':
                            sub_docs2.append(doc)
                    
                except Exception as err:
                    print(err)

            count = len(sub_docs)
            df.loc[date, et] = count

            count2 = len(sub_docs2)
            df2.loc[date, et] = count2
            
    path1 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Civic_Related/'
    path2 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_New/{country_name}/{today.year}_{today.month}_{today.day}/Non_Civic_Related/'
    
    if not os.path.exists(path1):
        Path(path1).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(path2):
        Path(path2).mkdir(parents=True, exist_ok=True)
    df.to_csv(path1 + f'{domain}.csv')
    df2.to_csv(path2 + f'{domain}.csv')

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
        ('Solomon Islands', 'SLB'),
        ("Costa Rica",'CRI'),
        ('Panama','PAN'),
        ('Mexico','MEX')

    ]


    # countries_needed = ['COL', 'ECU',  'PRY','JAM','HND', 'SLV', 'NIC','PER', 'DOM','PAN','CRI','SLB',]
    countries_needed = [
        
                            # 'COL', 'ECU',  'PRY','JAM','HND', 'SLV', 'NIC','PER', 'DOM','PAN', 'CRI','SLB', 
                            # 'BGD','NGA','UGA',
                            #    'ALB', 'BEN', 'ETH', 'GEO', 'KEN', 'MLI', 'MAR',   
                            #    'SRB', 'SEN', 'TZA', 'UKR', 'ZWE', 'MRT', 'ZMB', 'XKX', 'NER',  
                            #     'PHL', 'GHA', 'RWA', 'GTM', 'BLR', 'KHM', 'COD', 'TUR', 
                            #    'ZAF', 'TUN', 'IDN', 'AGO', 'ARM', 'LKA', 'MYS', 'CMR', 'HUN', 'MWI', 
                            #    'UZB', 'IND', 'MOZ', 'AZE', 'KGZ', 'MDA', 'KAZ', 'DZA', 'MKD', 'SSD', 
                            #    'LBR', 'PAK', 'NPL', 'NAM', 'BFA', 'TLS', #'MEX'
                            'MEX','UZB',
                            'IND'
                               ]

    countries = [(name, code) for (name, code) in all_countries if code in countries_needed]


    
    for ctup in countries:
        # time.sleep(500)

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
        ind = 1
        while ind:
            try:
                p_umap(count_domain_loc, [uri]*len(loc), loc, [country_name]*len(loc), [country_code]*len(loc), num_cpus=10)
                ind = 0
            except Exception as err:
                print('failed: ', err)
                pass
            
        ind = 1
        while ind:
            try:
                p_umap(count_domain_int, [uri]*len(ints), ints, [country_name]*len(ints), [country_code]*len(ints), num_cpus=10)
                ind = 0
            except Exception as err:
                print('failed: ', err)
                pass
        ind = 1
        while ind:
            try:
                p_umap(count_domain_int, [uri]*len(regionals), regionals, [country_name]*len(regionals), [country_code]*len(regionals), num_cpus=10)
                ind = 0
            except Exception as err:
                print('failed: ', err)
                pass
        
        # Git operations
        countries_added = '/'.join(countries_needed)
        commit_message = f"civic related count ({countries_added}) update"
        run_git_commands(commit_message)

# screen -S screen_count
# screen -r screen_count
# conda activate peace

# cd /home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts



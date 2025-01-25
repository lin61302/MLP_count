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
ind_dic = {'Yes':'Civic_related','No':'Non_Civic_Realed'}
events = [k for k in db.models.find_one({'model_name': 'civic_new'}).get('event_type_nums').keys()] + ['defamationcase']

# START WITH THE LOCALS
def count_domain_loc(uri, domain, country_name, country_code):

    

    db = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='D')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]
    df['day'] = [dd.day for dd in df.index]

    prev_colname = ''

    

    for et in events:
        df[et] = [0] * len(df)
    
    df2 = df.copy()
    df3 = df.copy()

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
                        'civic_related': {'$exists': True},
                        'event_type_civic_new': {'$exists': True},
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
                    
                except:
                    print(doc)


            count = len(sub_docs)
            df.loc[date, et] = count

            count2 = len(sub_docs2)
            df2.loc[date, et] = count2

            count3 = count+count2
            df3.loc[date, et] = count3

            


        
    path1 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_Daily/2023-5-civic_new/{country_name}/{today.year}_{today.month}_{today.day}/Civic_Related/'
    path2 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_Daily/2023-5-civic_new/{country_name}/{today.year}_{today.month}_{today.day}/Non_Civic_Related/'
    path3 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_Daily/2023-5-civic_new/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'
    
    if not os.path.exists(path1):
        Path(path1).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(path2):
        Path(path2).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(path3):
        Path(path3).mkdir(parents=True, exist_ok=True)
    df.to_csv(path1 + f'{domain}.csv')
    df2.to_csv(path2 + f'{domain}.csv')
    df3.to_csv(path3 + f'{domain}.csv')

def count_domain_int(uri, domain, country_name, country_code):

    db = MongoClient(uri).ml4p
    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='D')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]
    df['day'] = [dd.day for dd in df.index]

    prev_colname = ''

    

    for et in events:
        df[et] = [0] * len(df)
    
    df2 = df.copy()
    df3 = df.copy()

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
                        'civic_related': {'$exists': True},
                        'event_type_civic_new': {'$exists': True},
                        'cliff_locations.' + country_code : {'$exists' : True}
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
                    
                except:
                    print(doc)

            count = len(sub_docs)
            df.loc[date, et] = count

            count2 = len(sub_docs2)
            df2.loc[date, et] = count2

            count3 = count+count2
            df3.loc[date, et] = count3

            
    path1 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_Daily/2023-5-civic_new/{country_name}/{today.year}_{today.month}_{today.day}/Civic_Related/'
    path2 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_Daily/2023-5-civic_new/{country_name}/{today.year}_{today.month}_{today.day}/Non_Civic_Related/'
    path3 = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Civic_Daily/2023-5-civic_new/{country_name}/{today.year}_{today.month}_{today.day}/Combined/'

    
    if not os.path.exists(path1):
        Path(path1).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(path2):
        Path(path2).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(path3):
        Path(path3).mkdir(parents=True, exist_ok=True)
    df.to_csv(path1 + f'{domain}.csv')
    df2.to_csv(path2 + f'{domain}.csv')
    df3.to_csv(path3 + f'{domain}.csv')
    
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
        # ('Mauritania', 'MRT'),
        # ('Zambia', 'ZMB'),
        # ('Kosovo', 'XKX'),
        # ('Niger', 'NER'),
        ('Jamaica', 'JAM'),
        # ('Honduras', 'HND'),
        # ('Philippines', 'PHL'),
        # ('Ghana', 'GHA'),
        # ('Rwanda','RWA'),
        # ('Guatemala','GTM'),
        # ('Belarus','BLR'),
        # ('Cambodia','KHM'),
        # ('DR Congo','COD'),
        # ('Turkey','TUR'),
        # ('Bangladesh', 'BGD'),
        # ('El Salvador', 'SLV'),
        # ('South Africa', 'ZAF'),
        # ('Tunisia','TUN'),
        # ('Indonesia','IDN'),
        # ('Nicaragua','NIC'),
        # ('Angola','AGO'),
        # ('Armenia','ARM'),
        # ('Sri Lanka', 'LKA'),
        # ('Malaysia','MYS'),
        # ('Cameroon','CMR'),
        # ('Hungary','HUN')
        # ('Malawi','MWI'),
        # ('Uzbekistan','UZB'),
        # ('India','IND'),
        # ('Mozambique','MOZ'),
        # ('Azerbaijan','AZE')
        # ('Kyrgyzstan','KGZ')
        # ('Moldova','MDA'),
        # ('Kazakhstan','KAZ')
        # ('Peru','PER')
        # ('Algeria','DZA')
    ]


    # Zambia, Kosovo, Mauritania, Bangladesh, Bolivia, Bosnia, Cambodia, CAR, DRC, El Salvador, Ghana, Guatemala, Honduras, Hungary, India, Indonesia, Iraq, Jamaica, Jordan, Kazakhstan, Liberia, Libya, Malawi, Malaysia, Mexico, Mongolia, Mozambique, Myanmar, Nepal, Nicaragua, Niger, Pakistan, Philippines, Rwanda, South Africa, South Sudan, Thailand, Yemen

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

# screen -S screen_count
# screen -r screen_count
# conda activate peace

# cd /home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts



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
# uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
# db = MongoClient(uri).ml4p
# # (?<![a-zA-Z])
# today = pd.Timestamp.now()

# events = ['economic_crisis', 'total_articles']

# # legal_re = re.compile(r'((?<![a-zA-Z])freedom.*|(?<![a-zA-Z])assembl.*|(?<![a-zA-Z])associat.*|(?<![a-zA-Z])term limit.*|(?<![a-zA-Z])independen.*|(?<![a-zA-Z])succession.*|(?<![a-zA-Z])demonstrat.*|(?<![a-zA-Z])crackdown.*|(?<![a-zA-Z])draconian|(?<![a-zA-Z])censor.*|(?<![a-zA-Z])authoritarian|(?<![a-zA-Z])repress.*|(?<![a-zA-Z])NGO(?<![a-zA-Z])|(?<![a-zA-Z])human rights)',flags=re.IGNORECASE)

# econ_re = re.compile(r'(economic cris(is|es)|(?<![a-zA-Z])recession|(?<![a-zA-Z])market crash|financial bailout|economic stimulus|economic stimuli|inflation, austerit.*|financial cris(is|es)|economic downturn|economic collaps.*|fiscal cris(is|es)|banking cris(is|es)|economic decline|economic instabilit.*|hyperinflation|balance of payments crisis|exchange rate cris(is|es)|negative growth|liquidity cris(is|es)|mortgage cris(is|es)|debt cris(is|es)|financial collapse|fiscal collapse|credit crunch|credit downgrade|stagflation|foreclosure rate|real estate bubble|financial meltdown|market turmoil|economic slowdown|investment slump|fiscal contraction|economic instabilit.*|financial distress|market volatilit.*|fiscal stimulus|fiscal stimuli|interest rate hike)',flags=re.IGNORECASE)
# exclude_re = re.compile(r'(2008 financial|2008 economic|(?<![a-zA-Z])sports(?<![a-zA-Z])|football(?<![a-zA-Z])|(?<![a-zA-Z])club(?<![a-zA-Z])|(?<![a-zA-Z])team(?<![a-zA-Z]))',flags=re.IGNORECASE)
# # For arrest: apprehend, captur*, custody, imprison, jail
# # For legal action: case, lawsuit, sue, suit, trial, court, charge, rule, sentence, judge
# # For purge: dismiss, sack, replace, quit

# __georgiapath_int__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_international.xlsx'
# __georgiapath_loc__ = '/home/ml4p/peace-machine/peacemachine/Georgia_filter_local.xlsx'

# geo_int = pd.read_excel(__georgiapath_int__)
# geo_loc = pd.read_excel(__georgiapath_loc__)
# g_int = geo_int['CompanyName'].str.strip()
# g_loc = geo_loc['CompanyName'].str.strip()
# for i, doc in enumerate(g_int):
#     g_int[i] = "(?<![a-zA-Z])" + g_int[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"
# for i, doc in enumerate(g_loc):
#     g_loc[i] = "(?<![a-zA-Z])" + g_loc[i][2:-2].rstrip().lstrip() + "(?<![a-zA-Z])"
# g_int_string = '|'.join(g_int)
# g_loc_string = '|'.join(g_loc)
# g_int_filter = re.compile(g_int_string,flags=re.IGNORECASE)
# g_loc_filter = re.compile(g_loc_string,flags=re.IGNORECASE)

# def check_georgia(doc, _domain):  
#     if _domain == 'loc':
#         try:
#             if bool(g_loc_filter.search(doc)):
#                 return False
#             else:
#                 return True
#         except:
#             return True
#     if _domain == 'int':
#         try:
#             if bool(g_int_filter.search(doc)):
#                 return False
#             else:
#                 return True
#         except:
#             return True





# # START WITH THE LOCALS
# def count_domain_loc(uri, domain, countries):

#     db = MongoClient(uri).ml4p
#     df = pd.DataFrame()
#     df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M')
#     df.index = df['date']
#     df['year'] = [dd.year for dd in df.index]
#     df['month'] = [dd.month for dd in df.index]

#     for et in events:
#         df[et] = [0] * len(df)

#     for date in df.index:
#         colname = f"articles-{date.year}-{date.month}"
        

#         count = db[colname].count_documents(
#             {
#                 'source_domain': domain,
#                 'include': True,
#                 'civic_new': {'$exists': True},
#                 'event_type_civic_new': 'martiallaw',
#                 '$or': [
#                     {'cliff_locations.' + country_code : {'$exists' : True}},
#                     {'cliff_locations' : {}}
#                 ]
#             }
#         )

            

#         df.loc[date, et] = count

#     # check if directory exists
#     path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Diego/{today.year}_{today.month}_{today.day}/'
    
#     if not os.path.exists(path):
#         Path(path).mkdir(parents=True, exist_ok=True)
#     df.to_csv(path + f'martiallaw_local.csv')


# # Then ints
# def count_domain_int(uri, domain, countries):

#     db = MongoClient(uri).ml4p
#     df = pd.DataFrame()
#     df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M')
#     df.index = df['date']
#     df['year'] = [dd.year for dd in df.index]
#     df['month'] = [dd.month for dd in df.index]

#     for et in events:
#         df[et] = [0] * len(df)

#     for date in df.index:
#         colname = f"articles-{date.year}-{date.month}"
        

#         count = db[colname].count_documents(
#             {
#                 'source_domain': domain,
#                 'include': True,
#                 'civic_new': {'$exists': True},
#                 'event_type_civic_new': 'martiallaw',
#                 '$or': [
#                     {'cliff_locations.' + country_code : {'$exists' : True}},
#                     {'cliff_locations' : {}}
#                 ]
#             }
#         )

            

#         df.loc[date, et] = count

#     # check if directory exists
#     path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Diego/{today.year}_{today.month}_{today.day}/'
    
#     if not os.path.exists(path):
#         Path(path).mkdir(parents=True, exist_ok=True)
#     df.to_csv(path + f'martiallaw_local.csv')

# if __name__ == "__main__":

#     slp = False
    
#     if slp:
#         t = 10800
#         print(f'start sleeping for {t/60} mins')
#         time.sleep(t)
    
#     countries = [
#         ('Albania', 'ALB'), 
#         ('Benin', 'BEN'),
#         ('Colombia', 'COL'),
#         ('Ecuador', 'ECU'),
#         ('Ethiopia', 'ETH'),
#         ('Georgia', 'GEO'),
#         ('Kenya', 'KEN'),
#         ('Paraguay', 'PRY'),
#         ('Mali', 'MLI'),
#         ('Morocco', 'MAR'),
#         ('Nigeria', 'NGA'),
#         ('Serbia', 'SRB'),
#         ('Senegal', 'SEN'),
#         ('Tanzania', 'TZA'),
#         ('Uganda', 'UGA'),
#         ('Ukraine', 'UKR'),
#         ('Zimbabwe', 'ZWE'),
#         ('Mauritania', 'MRT'),
#         ('Zambia', 'ZMB'),
#         ('Kosovo', 'XKX'),
#         ('Niger', 'NER'),
#         ('Jamaica', 'JAM'),
#         ('Honduras', 'HND'),
#         ('Philippines', 'PHL'),
#         ('Ghana', 'GHA'),
#         ('Rwanda','RWA'),
#         ('Guatemala','GTM'),
#         ('Belarus','BLR'),
#         ('Cambodia','KHM'),
#         ('DR Congo','COD'),
#         ('Turkey','TUR'),
#         ('Bangladesh', 'BGD'),
#         ('El Salvador', 'SLV'),
#         ('South Africa', 'ZAF'),
#         ('Tunisia','TUN'),
#         ('Indonesia','IDN'),
#         ('Nicaragua','NIC'),
#         ('Angola','AGO'),
#         ('Armenia','ARM'),
#         ('Sri Lanka', 'LKA'),
#         ('Malaysia','MYS'),
#         ('Cameroon','CMR'),
#         ('Hungary','HUN'),
#         ('Malawi','MWI'),
#         ('Uzbekistan','UZB'),
#         ('India','IND'),
#         ('Mozambique','MOZ'),
#         ('Azerbaijan','AZE'),
#         ('Kyrgyzstan','KGZ'),
#         ('Moldova','MDA'),
#         ('Kazakhstan','KAZ'),
#         ('Peru','PER'),
#         ('Algeria','DZA'),
#         ('Macedonia','MKD'), 
#         ('South Sudan','SSD'),
#         ('Liberia','LBR'),
#         ('Pakistan','PAK'),
#         ('Nepal', 'NPL'),
#         ('Namibia','NAM'),
#         ('Burkina Faso', 'BFA'),
#         ('Dominican Republic', 'DOM')

#     ]



import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os

def connect_to_db(uri):
    try:
        client = MongoClient(uri)
        return client.ml4p
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None

def get_domains(db, country_code):
    loc_cursor = db['sources'].find({'primary_location': {'$in': [country_code]}, 'include': True})
    local = [doc['source_domain'] for doc in loc_cursor]
    
    # Append 'kaztag.kz' to local domains if country code is 'KAZ'
    if country_code == 'KAZ':
        local.append('kaztag.kz')
    elif country_code == 'XKX':
        local.append('balkaninsight.com')
    

    international = [doc['source_domain'] for doc in db['sources'].find({'major_international': True, 'include': True})]
    regional = [doc['source_domain'] for doc in db['sources'].find({'major_regional': True, 'include': True})]

    return local, international, regional

def create_date_range():
    today = datetime.today()
    start_date = datetime(2012, 1, 1)
    end_date = today + pd.Timedelta(days=31)
    date_range = pd.date_range(start_date, end_date, freq='M')
    return date_range

def populate_dataframe(db, date_range, domains, country_code, local=True):
    df = pd.DataFrame(index=date_range)
    df['year'] = df.index.year
    df['month'] = df.index.month
    

    for date in date_range:
        colname = f"articles-{date.year}-{date.month}"
        # query = {
        #     'source_domain': {'$in': domains},
        #     'include': True,
        #     'civic_new': {'$exists': True},
        #     'event_type_civic_new': 'martiallaw'
        # }
        # if local:
        #     query = {'source_domain': {'$in': domains},
        #     'include': True,
        #     'civic_new': {'$exists': True},
        #     'event_type_civic_new': 'martiallaw',
        #     '$or': [{'cliff_locations.' + country_code : {'$exists' : True}},{'cliff_locations' : {}}]}
        # else:
        #     query = {'source_domain': {'$in': domains},
        #     'include': True,
        #     'civic_new': {'$exists': True},
        #     'event_type_civic_new': 'martiallaw',
        #     'cliff_locations.' + country_code : {'$exists' : True}}
        if local:
            query1 = {'source_domain': {'$in': domains},
            'include': True,
            'civic_new': {'$exists': True},
            'event_type_civic_new': {'$exists': True},
            '$and': [{'$or': [{'event_type_civic_new': 'corruption'},{'event_type_civic_new_2':'corruption'}]},
                 {'$or': [{'cliff_locations.' + country_code: {'$exists': True}},{'cliff_locations': {}}]}]
            }
            
            query2 = {'source_domain': {'$in': domains},
            'include': True,
            'civic_new': {'$exists': True},
            'event_type_civic_new': {'$exists': True},
            '$or': [{'cliff_locations.' + country_code: {'$exists': True}},{'cliff_locations': {}}]
            }
        else:
            query1 = {'source_domain': {'$in': domains},
            'include': True,
            'civic_new': {'$exists': True},
            '$or': [{'event_type_civic_new': 'corruption'},{'event_type_civic_new_2':'corruption'}],
            'cliff_locations.' + country_code : {'$exists' : True}
            }

            query2 = {'source_domain': {'$in': domains},
            'include': True,
            'civic_new': {'$exists': True},
            'event_type_civic_new': {'$exists': True},
            'cliff_locations.' + country_code : {'$exists' : True}
            }

        

        # count1 = db[colname].count_documents(query1)
        count2 = db[colname].count_documents(query2)
        # if count2== 0:
        #     count = 0
        # else:    
        #     count = count1/ count2

        
        # df.loc[date, country_code] = count
        df.loc[date, country_code] = count2
        # df2.loc[date, country_code] = count2
    # return df
    return df


def save_dataframe(df, directory, filename):
    if not os.path.exists(directory):
        os.makedirs(directory)
    df.to_csv(os.path.join(directory, filename))

def main(uri, countries):
    db = connect_to_db(uri)
    if db:
        # Sort countries by country code (second item in tuple)
        sorted_countries = sorted(countries, key=lambda x: x[1])

        date_range = create_date_range()
        for local in [True, False]:  # Process both local and international
            combined_df = pd.DataFrame(index=date_range)
            combined_df['year'] = combined_df.index.year
            combined_df['month'] = combined_df.index.month
            

            for name, code in sorted_countries:
                print(f'Starting: {name}')
                local_domains, international_domains, regional_domains = get_domains(db, code)
                domains = local_domains if local else international_domains + regional_domains
                country_df = populate_dataframe(db, date_range, domains, code, local)
                combined_df = pd.concat([combined_df, country_df[code]], axis=1)
                

            today = datetime.today()

            directory = f"/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Diego/{today.year}_{today.month}_{today.day}"
            # filename = f"standardized_corruption_{'local' if local else 'international'}.csv"
            # save_dataframe(combined_df, directory, filename)
            # filename = f"total_corruption_{'local' if local else 'international'}.csv"
            filename = f"total_{'local' if local else 'international'}.csv"
            save_dataframe(combined_df, directory, filename)

            # filename = f"total_{'local' if local else 'international'}.csv"
            # save_dataframe(combined_df_total, directory, filename)

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
    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
    main(uri, countries)

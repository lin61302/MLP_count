import os
import getpass
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateparser
import json
from tqdm import tqdm
from p_tqdm import p_umap
from pymongo import MongoClient
from pymongo.errors import CursorNotFound
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv
from peacemachine.helpers import regex_from_list
from peacemachine.helpers import download_url


load_dotenv()
uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
db = MongoClient(uri).ml4p

def remove_blacklist(col_name, source_domains):
    
    sources = db.sources.find(
        {   
            'source_domain' : {'$in' : source_domains},
            'blacklist_url_patterns': {'$ne': []}
        }
    )
    for source in sources:
        while True:
            try:
                black = source.get('blacklist_url_patterns')
                black_re = regex_from_list(black)

                db[col_name].delete_many(
                    {
                        'source_domain': source.get('source_domain'),
                        'url': {'$regex': black_re}
                    }
                )
            except CursorNotFound:
                print('Cursor error, restarting')
                continue

            break


def include_sources(colname, source_domains):
    
    for source in source_domains:
        db[colname].update_many(
            {
                'source_domain': source
            },
            {
                '$set': {
                    'include': True
                }
            }
        )
    
# def add_whitelist(colname, source_domains):
#     """
#     filters whitelist sites
#     """

#     # sources without whitelists first
#     sources = db.sources.find(
#         {
#             # 'major_international' : True,
#             # 'include': True,
#             # 'source_domain' : 'nytimes.com',
#             'source_domain' : {'$in' : source_domains},
#             '$or': [
#                 {'whitelist_url_patterns': []},
#                 {'whitelist_url_patterns': {'$exists': False}}
#             ]
#         }
#     )
#     for source in sources:
#         # deal with the sources that don't have a whitelist
#         if not(source.get('whitelist_url_patterns')):
#             db[colname].update_many(
#                 {
#                     'source_domain': source.get('source_domain')
#                 },
#                 {
#                     '$set': {
#                         'include': True
#                     }
#                 }
#             )

#     # sources with whitelists next
#     sources = db.sources.find(
#         {   
#             # 'major_international' : True,
#             # 'include': True,
#             # 'source_domain' : 'nytimes.com',
#             'source_domain' : {'$in' : source_domains},
#             '$or': [
#                 {'whitelist_url_patterns': {'$ne': []}}
#             ]
#         }
#     )
#     for source in sources:
#         source_regex = regex_from_list(source.get('whitelist_url_patterns'), compile=False)
#         db[colname].update_many(
#             {
#                 'source_domain': source.get('source_domain'),
#                 'url': {'$regex': source_regex}
#             },
#             {
#                 '$set': {
#                     'include': True
#                 }
#             }
#         )
    

# def add_otherlist(colname, source_domains):
    # """
    # doesn't delete but doesn't include sites with other_url_patterns
    # """

    # sources = db.sources.find(
    #     {
    #         # 'major_international' : True,
    #         # 'include' : True,
    #         # 'source_domain' : 'nytimes.com',
    #         'source_domain' : {'$in' : source_domains},
    #         'other_url_patterns': {
    #             '$exists': True,
    #             '$ne': []
    #         }
    #     }
    # )

    # for source in sources:
    #     source_regex = regex_from_list(source.get('other_url_patterns'), compile=False)
    #     db[colname].update_many(
    #         {
    #             'source_domain': source.get('source_domain'),
    #             'url': {'$regex': source_regex}
    #         },
    #         {
    #             '$set': {
    #                 'include': False
    #             }
    #         }
    #     )


def create_year_month(colname, source_domains):
    print("Collection", colname)
    cursor = db[colname].find(
        {
            'include' : True,
            'source_domain' : {'$in' : source_domains},
            'date_publish': {
                '$exists': True,
                '$type': 'date'
            },
            'month': {'$exists': False}
        }
    )
    for doc in cursor:
        db[colname].update_one(
            {'_id': doc['_id']},
            {
                '$set':{
                    'year': doc['date_publish'].year,
                    'month': doc['date_publish'].month
                }
            }
        )


def dedup_collection(colname, source_domains):

    cursor = db[colname].find({'source_domain': {'$in': source_domains}, 'include' : True}, batch_size=1)

    mod_count = 0

    for _doc in cursor:
        try:
            start = _doc['date_publish'].replace(hour=0, minute=0, second=0)
            cur = db[colname].find(
                {   
                    'source_domain': _doc['source_domain'],
                    'date_publish': {'$gte': start},
                    '_id': {'$ne': _doc['_id']},
                    'title': _doc['title']
                }
            )
            for _doc in cur:
                try:
                    db['deleted-articles'].insert_one(_doc)
                except DuplicateKeyError:
                    pass
            
            res = db[colname].delete_many(
                {   
                    'source_domain': _doc['source_domain'],
                    'date_publish': {'$gte': start},
                    '_id': {'$ne': _doc['_id']},
                    'title': _doc['title']
                }
            )

            if res.deleted_count != 0:
                mod_count += res.deleted_count
        except:
            pass
    
    print(f'{colname} DELETED: {mod_count}')


def set_none_language(colname, sd):
    cur = db[colname].find({'include' : True, 'source_domain' : sd, 'language' : None})
    for doc in cur:
        try:
            db[colname].update_one(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'language' : 'en',
                    }
                }
            )
        except KeyError:
            pass

def set_xinhuanet_language(colname, sd, url_re, lang):
    cur = db[colname].find({'include' : True, 'source_domain' : sd, 'language' : None, 'url' : {'$regex' : url_re}})
    for doc in cur:
        try:
            db[colname].update_one(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'language' : lang,
                    }
                }
            )
        except KeyError:
            pass


def set_kosovasot_language(colname, sd):
    cur = db[colname].find({'include' : True, 'source_domain' : sd, 'language' : 'en'})
    for doc in cur:
        try:
            db[colname].update_one(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'language' : 'sq',
                    }
                }
            )
        except KeyError:
            pass

def set_language_params(colname):
    print(colname)
    cur = db[colname].find({'include' : True, 'language': 'en', 'language_translated' : {'$exists' : False}})
    for doc in cur:
        try:
            db[colname].update_one(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'language_translated': 'en',
                        'title_translated': doc['title'],
                        'maintext_translated': doc['maintext']
                    }
                }
            )
        except KeyError:
            pass


if __name__ == "__main__":
    colnames = [ll for ll in db.list_collection_names() if ll.startswith('articles-')]
    colnames = [ll for ll in colnames if ll != 'articles-nodate']
    colnames = [ll for ll in colnames if int(ll.split('-')[1]) >= 2012 and int(ll.split('-')[1]) <= 2021]
    
    # sort by most recent
    colnames = sorted(colnames, key = lambda x: (int(x.split('-')[1]), int(x.split('-')[2])), reverse=True)
    # colnames = [ll for ll in colnames if ll.startswith('articles-2021')] #use this from 2021 articles onwards for old countries
    
    source_domains = ['tribune.net.ph']
    #source_domains += db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['NGA']}})
    #source_domains += db.sources.distinct('source_domain', filter={'include':True, 'major_international':True})
    #source_domains += db.sources.distinct('source_domain', filter={'include':True, 'major_regional':True})
    # source_domains += db.sources.distinct('source_domain', filter={'include':True})

    
    print('STARTING BLACKLIST')
    for colname in colnames:
        remove_blacklist(colname, source_domains)
    
    print('INCLUDING SOURCES')
    for colname in colnames:
        include_sources(colname, source_domains)
    
    print('DE-DUPLICATION PROCESS')
    for colname in colnames:
        dedup_collection(colname, source_domains)

    print('CREATING YEAR AND MONTH FIELDS')
    for colname in colnames:
        create_year_month(colname, source_domains)
    
    print('SETTING LANGUAGE KEYS FOR GEORGIATODAY')
    for colname in colnames:
        set_none_language(colname, 'georgiatoday.ge')

    print('SETTING LANGUAGE KEYS FOR KOSOVA-SOT')
    for colname in colnames:
        print(colname)
        set_kosovasot_language(colname, 'kosova-sot.info')

    print('SETTING LANGUAGE KEYS FOR JAMAICAOBSERVER')
    for colname in colnames:
        print(colname)
        set_none_language(colname, 'jamaicaobserver.com')

    print('SETTING LANGUAGE KEYS FOR XINHUANET')
    for colname in colnames:
        print(colname)
        set_xinhuanet_language(colname, 'xinhuanet.com', 'xinhuanet.com/english', 'en')
        set_xinhuanet_language(colname, 'xinhuanet.com', 'gz.xinhuanet.com', 'zh')

    print('SETTING LANGUAGE KEYS FOR ENGLISH SOURCES')
    for colname in colnames:
        set_language_params(colname)

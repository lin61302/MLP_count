import random
import sys
import os
import re
import subprocess
from p_tqdm import p_umap
from tqdm import tqdm
import ast
from pymongo import MongoClient
import random
from urllib.parse import urlparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from dotenv import load_dotenv
from pymongo.errors import DuplicateKeyError
from urllib.parse import urljoin, urlparse
import requests
from pymongo.errors import CursorNotFound
from peacemachine.helpers import urlFilter
from newsplease import NewsPlease

def download_url(uri, url, download_via=None, insert=True, overwrite=False):
    """
    process and insert a single url
    """
    db = MongoClient(uri).ml4p

    try:
        # download
        header = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        }
        response = requests.get(url, headers=header)
        # process
        article = NewsPlease.from_html(response.text, url=url).__dict__
        # add on some extras
        article['date_download']=datetime.now()
        if download_via:
            article['download_via'] = download_via
        # insert into the db
        if not insert:
            return article
        if article:
            try:
                year = article['date_publish'].year
                month = article['date_publish'].month
                colname = f'articles-{year}-{month}'
            except:
                colname = 'articles-nodate'
            try:
                if overwrite:
                    db[colname].replace_one(
                        {'url': url},
                        article,
                        upsert=True
                    )
                else:
                    db[colname].insert_one(
                        article
                    )
                db['urls'].insert_one({'url': article['url']})
                print("Inserted! in ", colname)
            except DuplicateKeyError:
                pass
        return article
    except Exception as err: # TODO detail exceptions
        print("ERRORRRR......", err)
        pass


def main():
    load_dotenv()
    uri = os.getenv('DATABASE_URL')
    db = MongoClient(uri).ml4p

    dates = pd.date_range('2012-1-1', datetime.now()+relativedelta(months=1), freq='M')
    batch_size = 128
    source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['ECU', 'SEN', 'ETH']}})
    #'KEN', 'NGA', 'TZA', 'ECU', 'COL', 'PRY', 'SEN', 'UGA', 'MAR', 'BEN', 'ETH', 'ALB', 'ZWE', 'UKR
    
    for date in dates:
        colname = f'{date.year}-{date.month}-events'
        try:
            cursor = db[colname].find({
                "source_domain" : {'$in' : source_domains}
                # 'source_domain' : {'$in' : ['washingtonpost.com', 'wsj.com','csmonitor.com','themoscowtimes.com','xinhuanet.com','scmp.com', 'aljazeera.com', 'reuters.com', 'bbc.com', 'nytimes.com']}
            }).batch_size(batch_size)

            list_urls = []
            for _doc in tqdm(cursor):
                if 'https://' in _doc['url']:
                    _doc['url'] =  _doc['url'][8:]
                if 'http://' not in _doc['url']:
                    _doc['url'] = 'http://' + _doc['url']
                list_urls.append(_doc['url'])
                if len(list_urls) >= batch_size:
                    print('Extracting urls')
                    try:
                        p_umap(download_url, [uri]*len(list_urls), list_urls, num_cpus=10)
                    except ValueError:
                        print('ValueError')
                    except AttributeError:
                        print('AttributeError')
                    except Exception as err:
                        print(err)
                    list_urls = []
            p_umap(download_url, [uri]*len(list_urls), list_urls, num_cpus=10)
            list_urls = []
        except CursorNotFound:
            pass


if __name__== '__main__':
    main()
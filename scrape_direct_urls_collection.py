import random
import sys
import os
import re
from p_tqdm import p_umap
from tqdm import tqdm
from pymongo import MongoClient
import random
from urllib.parse import urlparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound
import requests
from peacemachine.helpers import urlFilter
from newsplease import NewsPlease
from dotenv import load_dotenv

def download_url(uri, url, download_via=None, insert=True, overwrite=False):
    """
    process and insert a single url
    """
    db = MongoClient(uri).ml4p

    try:
        # download
        #print('First Try ', url)
        #header = {
        #    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        #}
        #header = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36''(KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36')}
        header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        response = requests.get(url, headers=header)
        # process
        article = NewsPlease.from_html(response.text, url=url).__dict__
        # add on some extras
        article['date_download']=datetime.now()
        if download_via:
            article['download_via'] = download_via
        # insert into the db
        if not insert:
            print("Not Inserted ", url)
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
                    print("Inserted! in ", colname)
                    db['direct-urls'].delete_one({
                        'url' : url
                    })
                db['urls'].insert_one({'url': article['url']})
                
            except DuplicateKeyError:
                print("DUPLICATE! deleting")
                db['direct-urls'].delete_one({
                    'url' : url
                })
        return article
    except Exception as err: 
        print("ERRORRRR......", err)
        pass


def main():
    load_dotenv()
    uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
    #uri = os.getenv('DATABASE_URL')
    db = MongoClient(uri).ml4p
    batch_size = 128
    
    colname = 'direct-urls'
    #source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : 'SEN'})
    source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional': True})
    sources_re = "|".join(source_domains)

    try:
        cursor = db[colname].find({
            # specific source domain 
            #'url': {'$regex': 'univision.com'}
            'url': {'$regex': 'neweasterneurope.eu'}
            #'url' : {'$regex' : 'portafolio.co'}
        }) 

        list_urls = []
        
        for _doc in tqdm(cursor):
            # print(_doc['url']) #make sure the urls are correct
            # modify direct URLs if needed
            if 'feed/' in _doc['url']:
                _doc['url'] = _doc['url'][:-5]
            if '\ufeff' in _doc['url']:
                _doc['url'] = _doc['url'].replace("\ufeff", "")
            if "https:/w" in _doc['url']:
                #print('yes')
                _doc['url'] = _doc['url'].replace("https:/w", "https://w")   
            list_urls.append(_doc['url'])
            print(_doc['url']) 
            
            if len(list_urls) >= batch_size:
                print('Extracting urls')
                try:
                    p_umap(download_url, [uri]*len(list_urls), list_urls, ['direct']*len(list_urls), num_cpus=10)
                except ValueError:
                    print('ValueError')
                except AttributeError:
                    print('AttributeError')
                except Exception as err:
                    print('Here:', err)
                list_urls = []
        p_umap(download_url, [uri]*len(list_urls), list_urls, ['direct']*len(list_urls), num_cpus=10)
        list_urls = []
    except CursorNotFound:
        print('cursor not found')
        pass

if __name__== '__main__':
    main()
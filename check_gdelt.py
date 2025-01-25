from p_tqdm import p_umap
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
from peacemachine.helpers import urlFilter
from newsplease import NewsPlease
from datetime import datetime
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv
import os

def gdelt_download(uri, n_cpu=0.5):
    pass


def download_url(uri, url, download_via=None, insert=True, overwrite=False):
    """
    process and insert a single url
    """
    db = MongoClient(uri).ml4p

    try:
        header = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        }
        response = requests.get(url, headers=header) 
        article = NewsPlease.from_html(response.text, url=url).__dict__
        article['date_download']=datetime.now()
        if download_via:
            article['download_via'] = download_via
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
                print("Inserted in ", colname, article['url'])
            except DuplicateKeyError:
                pass
        return article
    except Exception as err: 
        pass

class GdeltDownloader:

    def __init__(self, uri, num_cpus):
        
        self.uri = uri
        self.num_cpus = num_cpus
        self.db = MongoClient(uri).ml4p
        self.source_domains = self.db.sources.distinct('source_domain', filter={'major_international' : True, 'include' : True})
        self.missing_domains = []

    def parse_file(self, gdelt_url):

        try:

            # self.missing_domains = self.db.sources.distinct('source_domain', filter={'major_international' : True, 'include' : True})
            
            # if self.missing_domains == None:
            #     return
            df = pd.read_table(gdelt_url, compression='zip', header=None)
            urls = df.iloc[:, -1]

            ufilter = urlFilter(self.uri)
            urls = ufilter.filter_list(urls)
            urls1 = []
            # for sd in self.missing_domains:
            #     urls1 += [url for url in urls if sd in url]
            p_umap(download_url, [self.uri]*len(urls1), urls1, ['gdelt']*len(urls1), num_cpus=4)
        
        except Exception as err:
            print('PARSING ERROR')
            pass

if __name__ == "__main__":

    load_dotenv()
    uri = os.getenv('DATABASE_URL')
    db = MongoClient(uri).ml4p
    gd = GdeltDownloader(uri, 1)

    url_re = '/gdeltv2/2021'
    
    cursor = db.gdelt.find({
        'url' : {'$regex' : url_re}
    })
    
    files = [c['url'] for c in cursor]
    
    count = len(files)
    start = 0
    for f in files:
        start+=1
        print("PARSING:", f, start, "of total: ", count)
        gd.parse_file(f)
        
        
        
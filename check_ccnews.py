from warcio.archiveiterator import ArchiveIterator
import urllib
from urllib.parse import urlparse
from urllib.request import urlretrieve
from tqdm import tqdm
import os
import subprocess
from p_tqdm import p_umap
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound
import requests
from newsplease import NewsPlease
from datetime import datetime
from dotenv import load_dotenv

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


def download(url):
  """
  Download and save a file locally.
  :param url: Where to download from
  :return: File path name of the downloaded file
  """
  url = os.getenv('AMAZON_CCNEWS_S3') + url
  local_filename = urllib.parse.quote_plus(url)
  local_filepath = url.split('/')[-1]
  try:
    urlretrieve(url, local_filepath)
  except urllib.error.HTTPError:
    pass
  return local_filepath

def pull_domain(url):
  domain = urlparse(url).netloc
  if domain.startswith('www.'):
    domain = domain[4:]
  return domain

def warc_domains(warc_path):
  urls = []
  warc_input = open(warc_path, 'rb')
  for record in tqdm(ArchiveIterator(warc_input)):
    if record.rec_type == 'response':
      if 'WARC-Target-URI' in record.rec_headers:
        url = record.rec_headers['WARC-Target-URI']
        urls.append(url)
  return urls

def warc_main(path):
  local_path = download(path)
  warc_info = warc_domains(local_path)
  os.remove(local_path)
  return warc_info

load_dotenv()

__AWSPATH__ = os.getenv('AWS_CMD')
cmd = __AWSPATH__ + '2021/ --no-sign-request'
#uri = os.getenv('DATABASE_URL')
uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
db = MongoClient(uri).ml4p

exitcode, stdout_data = subprocess.getstatusoutput(cmd)

if exitcode > 0:
  raise Exception(stdout_data)

lines = stdout_data.splitlines()
files = []

for line in lines:
  files += [line.split(" ")[-1]]

master_urls = []

#source_domains = db.sources.distinct('source_domain', filter={'include' : True})
source_domains = db.sources.distinct('source_domain', filter = {'include' : True, 'major_internatonal' : True})
# source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['NGA', 'TZA', 'ECU', 'SEN', 'UGA', 'MAR', 'BEN', 'ETH', 'ZWE', 'UKR', 'SRB', 'HND', 'RWA', 'JAM']}})
# source_domains = ['jamaica-gleaner.com', 'thestandard.co.zw', 'chronicle.co.zw', 'gazetatema.net']

for ff in tqdm(files):
  gaz = []
  master_urls = warc_main(ff)
  for src in source_domains:
    gaz += [url for url in master_urls if src in url]
  
  p_umap(download_url, [uri]*len(gaz), gaz, ['ccnews']*len(gaz), num_cpus=10)



#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Oct 18 2021

@author: diegoromero

This script obtains urls from from specific sections and the archive of the
specified source and inserts them directly into the relevant collections
within the db using newsplease.

"""

import random
import sys
import os
import re
import getpass
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np 
import plotly.express as px
from tqdm import tqdm

from pymongo import MongoClient


import bs4
from bs4 import BeautifulSoup
from newspaper import Article
from dateparser.search import search_dates
import dateparser
import requests
from urllib.parse import quote_plus

import urllib.request
from time import sleep
from time import time
from random import randint
from warnings import warn
import json
import pandas as pd
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



## NEED TO DEFINE SOURCE!
source = "addisfortune.news"


# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

# headers for scraping
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}



## COLLECTING URLS FROM SECTIONS + ARCHIVE
urls = []

# fortune-news 100
for i in range(1, 100):
    if i == 1:
        url = 'https://addisfortune.news/fortune-news/'
    else:
        url = 'https://addisfortune.news/fortune-news/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

# archive 172
for i in range(1, 172):
    if i == 1:
        url = 'https://archive.addisfortune.news/content/fortune-news/'
    else:
        url = 'https://archive.addisfortune.news/content/fortune-news/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

list_urls = list(set(urls))

print("Total number of urls found: ", len(list_urls))

#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Dropbox/addisfortune_Test.csv')  


## INSERTING IN THE DB:
url_count = 0
for url in list_urls:
    if url == "":
        pass
    else:
        if url == None:
            pass
        else:
            if "/columns/" in url:
                pass
            else:
                if "/advertise-with-us" in url:
                    pass
                else:
                    if "/letter-to-the-editor" in url:
                        pass
                    else:
                        if "addisfortune.news" in url:
                            print(url, "FINE")
                            ## SCRAPING USING NEWSPLEASE:
                            try:
                                #header = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36''(KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36')}
                                header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                                response = requests.get(url, headers=header)
                                # process
                                article = NewsPlease.from_html(response.text, url=url).__dict__
                                # add on some extras
                                article['date_download']=datetime.now()
                                article['download_via'] = "Direct"
                                article['source_domain'] = source
                                # insert into the db
                                try:
                                    year = article['date_publish'].year
                                    month = article['date_publish'].month
                                    colname = f'articles-{year}-{month}'
                                    #print(article)
                                except:
                                    colname = 'articles-nodate'
                                try:
                                    #TEMP: deleting the stuff i included with the wrong domain:
                                    #myquery = { "url": final_url, "source_domain" : 'web.archive.org'}
                                    #db[colname].delete_one(myquery)
                                    # Inserting article into the db:
                                    db[colname].insert_one(article)
                                    print("Inserted! in ", colname)
                                    # count:
                                    url_count = url_count + 1
                                    #db['urls'].insert_one({'url': article['url']})
                                except DuplicateKeyError:
                                    print("DUPLICATE! Not inserted.")
                            except Exception as err: 
                                print("ERRORRRR......", err)
                                pass
                        else:
                            pass



print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")

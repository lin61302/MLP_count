#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 11 2021

@author: diegoromero

This script updates aljazeera.com using daily sitemaps.
It can be run as often as one desires. 
 
"""
# Packages:
import sys
import os
import re
import getpass
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np 
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
import time
from time import time
from random import randint, randrange
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
#from dotenv import load_dotenv

# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

# headers for scraping
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


## COLLECTING URLS
urls = []

## NEED TO DEFINE SOURCE!
source = 'aljazeera.com'
#https://www.aljazeera.com/sitemap.xml?yyyy=2021&mm=8&dd=12
## STEP 0: Define dates
yearn = "2021"
monthn = "10"
daystart = "1"
dayend = "31" 

for i in range(int(daystart), int(dayend)+1):
    url = "https://www.aljazeera.com/sitemap.xml?yyyy=" + yearn + "&mm=" + monthn + "&dd=" + str(i) 
    print("Extracting from: ", url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    for link in soup.findAll('loc'):
        urls.append(link.text)
    for link in soup.find_all('a'):
        urls.append(link.get('href')) 
    print("URLs so far: ", len(urls))


# STEP 1: Get rid or urls from blacklisted sources
blpatterns = ['/opinions/', '/sports/', '/author/', '/videos/', '/gallery/', '/inpictures/', '/watch/', '/shows/', '/blogs/', '/tag/', '/podcasts/', '/program/', '/schedule/', '/where/']

clean_urls = []
for url in urls:
    if "aljazeera.com" in url:
        count_patterns = 0
        for pattern in blpatterns:
            if pattern in url:
                count_patterns = count_patterns + 1
        if count_patterns == 0:
            clean_urls.append(url)
    else:
        indexd = url.index("/")
        if indexd == 0:
            newurl = "https://www.aljazeera.com" + url 
            clean_urls.append(newurl)
# List of unique urls:
list_urls = list(set(clean_urls))

# Manually check urls:
#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Downloads/test.csv')  

print("Total number of USABLE urls found: ", len(list_urls))


## INSERTING IN THE DB:
url_count = 0
for url in list_urls:
    if url == "":
        pass
    else:
        if url == None:
            pass
        else:
            if "aljazeera.com" in url:
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
                    article['download_via'] = "Direct2"
                    
                    ## Fixing Date:
                    #soup = BeautifulSoup(response.content, 'html.parser')

                    #try:
                    #    contains_date = soup.find("div", {"class":"post-meta-date"}).text
                        #contains_date = soup.find("i", {"class":"fa fa-calendar"}).text
                    #    article_date = dateparser.parse(contains_date, date_formats=['%d/%m/%Y'])
                    #    article['date_publish'] = article_date
                    #except:
                    #    article_date = article['date_publish']
                    #    article['date_publish'] = article_date

                    ## Inserting into the db
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
                        # count:
                        url_count = url_count + 1
                        #print(article['date_publish'])
                        #print(article['date_publish'].month)
                        #print(article['title'])
                        #print(article['maintext'])
                        print("Inserted! in ", colname, " - number of urls so far: ", url_count, "of ", len(list_urls))
                        db['urls'].insert_one({'url': article['url']})
                    except DuplicateKeyError:
                        print("DUPLICATE! Not inserted.")
                except Exception as err: 
                    print("ERRORRRR......", err)
                    pass
            else:
                pass


print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")
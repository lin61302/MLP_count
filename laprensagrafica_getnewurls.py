#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 29 2021

@author: diegoromero

This script updates laprensagrafica.com using historical sitemaps.
It can be run whenever necessary. 
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
from dotenv import load_dotenv

# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

# headers for scraping
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


## COLLECTING URLS
urls = []
siteurls = []

## NEED TO DEFINE SOURCE!
source = 'laprensagrafica.com'

# STEP 0: Get sitemap urls:
# Note: to update, comment lines 70-92 and uncomment lines 95-101

# Extracting urls from general sitemap:
url = "https://www.laprensagrafica.com/sitemaps/index.xml"
print("Extracting from: ", url)
reqs = requests.get(url, headers=headers)
soup = BeautifulSoup(reqs.text, 'html.parser')
for link in soup.findAll('loc'):
    sm_url = link.text
    if '/videos/' in sm_url:
        pass
    else:
        siteurls.append(sm_url)

print("Number of sitemaps: ", len(siteurls))

# STEP 1: Get urls of articles from sitemaps (txt files):
import urllib
for sitmp in siteurls:
    print("Extracting from: ", sitmp)
    file = urllib.request.urlopen(sitmp)
    for line in file:
        decoded_line = line.decode("utf-8")
        urls.append(decoded_line)

    print("URLs so far: ", len(urls))

# STEP 0 -> UPDATE GETTING URLS FROM THE LATEST SITEMAP 
#sitemapurl = "https://www.laprensagrafica.com/_static/sitemaps/videos/lpg-1.txt" # CHANGE if necessary
#print("Extracting from: ", sitemapurl)
#file = urllib.request.urlopen(sitmp)
#for line in file:
#    decoded_line = line.decode("utf-8")
#    urls.append(decoded_line)
#print("URLs so far: ", len(urls))

# Manually check urls:
#list_urls = list(set(urls))
#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Downloads/test.csv')  
#print("DONE")



# STEP 2: Get rid or urls from blacklisted sources
blpatterns = ['/deportes/', '/opinion/', '/ella/', '/farandula/', '/salud/', '/tendencias/', '/blogs.', '/tag/', '/img/', '/7s.', '/Meme-', '/mujer/', '/revistas/']
clean_urls = []
for url in urls:
    if "laprensagrafica.com" in url:
        count_patterns = 0
        for pattern in blpatterns:
            if pattern in url:
                count_patterns = count_patterns + 1
        if count_patterns == 0:
            clean_urls.append(url)
# List of unique urls:
list_urls = list(set(clean_urls))

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
            if "laprensagrafica.com" in url:
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
                    
                    ## Fixing Date + Title + Text
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Get Title: 
                    try:
                        #article_title = soup.find("title").text
                        contains_title = soup.find("meta", {"property":"og:title"})
                        article_title = contains_title['content']
                    except:
                        article_title = None
                    
                    if article['title'] == None:
                        article['title'] = article_title
                        
                    # Get Main Text:
                    try:
                        maintext = soup.find("div", {"class":"news-body"}).text
                    except: 
                        maintext = None

                    if maintext != None:
                        article['maintext'] = maintext.strip()

                    # Get Date
                    try: 
                        contains_date = soup.find("time", {"class":"news-date"})
                        contains_date = contains_date['datetime']
                        article_date = dateparser.parse(contains_date,date_formats=['%d/%m/%Y'])
                        article['date_publish'] = article_date  
                    except:
                        try:
                            contains_date = soup.find("meta", {"name":"cXenseParse:recs:publishtime"})
                            contains_date = contains_date['content']
                            article_date = dateparser.parse(contains_date,date_formats=['%d/%m/%Y'])
                            article['date_publish'] = article_date  
                        except:
                            article_date = article['date_publish']
                            article['date_publish'] = article_date 

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
                        #print(article['date_publish'], " MONTH: ", article['date_publish'].month)
                        #print(article['date_publish'].month)
                        #print(article['title'][0:100])
                        #print(article['maintext'][0:200])
                        print("Inserted! in ", colname, " - number of urls so far: ", url_count, " of ", len(list_urls))
                        db['urls'].insert_one({'url': article['url']})
                    except DuplicateKeyError:
                        print("DUPLICATE! Not inserted.")
                except Exception as err: 
                    print("ERRORRRR......", err)
                    pass
            else:
                pass


print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")
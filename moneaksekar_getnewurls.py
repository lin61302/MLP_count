#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 26 2021

@author: diegoromero

This script updates 'moneaksekar.com' using section archives.
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
from dotenv import load_dotenv

# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

# headers for scraping
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

## COLLECTING URLS
urls = []

## NEED TO DEFINE SOURCE!
source = 'moneaksekar.com'

# STEP 0: Get articles from sections:
# sections:
sections = ['report','national-news','police','international-news']
# CHANGE endnumbers for each section in accordance with how far 
# back you need to go:
endnumber = ['78','2','2','2'] 

for section in sections:
    indexword = sections.index(section)
    endnumberx = endnumber[indexword]

    for i in range(1, int(endnumberx)+1):
        if i == 1:
            url = "http://www.moneaksekar.com/category/" + section + "/"
            #+ ".aspx"   
        else:
            url = "http://www.moneaksekar.com/category/" + section + "/page/" + str(i) + "/"
        
        print("Section: ", section, " -> URL: ", url)

        reqs = requests.get(url, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        for link in soup.find_all('a'):
            urls.append(link.get('href')) 
        print("URLs so far: ", len(urls))


# General section:
for i in range(1, 78):
    if i == 1:
        url = "http://www.moneaksekar.com/"
        #+ ".aspx"   
    else:
        url = "http://www.moneaksekar.com/page/" + str(i) + "/"
    
    print("Section: GENERAL -> URL: ", url)

    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 
    print("URLs so far: ", len(urls))

# STEP 1: Keep unique, useful urls:
dedup = list(set(urls))
list_urls  = []
for url in dedup:
    if "/#" in url:
        pass
    else:
        if '/category/' in url:
            pass
        else:
            if 'moneaksekar.com' in url:
                if '.html/' in url:
                    list_urls.append(url)

# Manually check urls:
#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Downloads/test.csv') 
#print("DONE")

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
            if 'moneaksekar.com' in url:
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
                    
                    ## Fixing key data:
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Fixing Title: 
                    if article['title'] == None:
                        try: 
                            #article_title = soup.find("title").text
                            contains_title = soup.find("meta", {"property":"og:title"})
                            article_title = contains_title["content"]
                            article['title']  = article_title   
                        except:
                            article['title']  = None
                    
                    # Fixing Main Text:
                    if article['maintext'] == None:  
                        try:
                            maintext_contains = soup.findAll("p")
                            #maintext_contains = soup.find("div", {"class":"entry-content"}).text
                            maintext = maintext_contains[0].text + " " + maintext_contains[1].text
                            article['maintext'] = maintext
                        except: 
                            article['maintext']  = None

                    # Fixing Date:
                    try: 
                        #contains_date = soup.find("time", {"class":"entry-date"})
                        contains_date = soup.find("meta", {"property":"og:description"})
                        datex = contains_date["content"]
                        datex = datex.replace(",", "")
                        #print(datex)
                        datevector = datex.split()
                        dayx = datevector[0]
                        monthx = datevector[1]
                        monthx = monthx.lower()
                        yearx = datevector[2]
                        #print(dayx, monthx, yearx)
                        months_en = ['january','february','march','april','may','june','july','august','september','october','november','december']
                        indexm = months_en.index(monthx)
                        article_date = datetime(int(yearx),int(indexm+1),int(dayx))
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
                        print(article['date_publish'])
                        #print(article['date_publish'].month)
                        print(article['title'][0:100])
                        print(article['maintext'][0:100])
                        print("Inserted! in ", colname, " - number of urls so far: ", url_count)
                        db['urls'].insert_one({'url': article['url']})
                    except DuplicateKeyError:
                        print("DUPLICATE! Not inserted.")
                except Exception as err: 
                    print("ERRORRRR......", err)
                    pass
            else:
                pass


print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")


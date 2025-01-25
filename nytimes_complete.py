#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 11 2021

@author: diegoromero

This script completes collections of previous months for
NYT.com using daily sitemaps. There should not be a need 
to run this script ever again unless we forget to update 
this source every month. 
 
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
source = 'nytimes.com'
#https://www.nytimes.com/issue/todayspaper/2021/09/01/todays-new-york-times
#https://www.nytimes.com/sitemap/2021/11/05/


## STEP 0: Define dates
yearmonths = ["/2020/11/", "/2020/12/", "/2021/01/", "/2021/02/", "/2021/03/", "/2021/04/", "/2021/05/", "/2021/06/", "/2021/07/"]
list31 = [1,3,5,7,8,10,12]
list30 = [4,6,9,11]

for ym in yearmonths:
    monthx = ym[len(ym)-3:len(ym)-1]
    print(ym, " and month: ", int(monthx))

    if monthx in list31:
        for i in range(1, 32):
            if i <10:
                daynum = "0" + str(i)
            else:
                daynum = str(i)

            url = "https://www.nytimes.com/sitemap" + ym + daynum + "/"
            print("Extracting from: ", url)
            reqs = requests.get(url, headers=headers)
            soup = BeautifulSoup(reqs.text, 'html.parser')
            #for link in soup.findAll('loc'):
            #    urls.append(link.text)
            for link in soup.find_all('a'):
                urls.append(link.get('href')) 
            print("URLs so far: ", len(urls))
    else:
        if monthx in list30:
            for i in range(1, 31):
                if i <10:
                    daynum = "0" + str(i)
                else:
                    daynum = str(i)
                url = "https://www.nytimes.com/sitemap" + ym + daynum + "/"
                print("Extracting from: ", url)
                reqs = requests.get(url, headers=headers)
                soup = BeautifulSoup(reqs.text, 'html.parser')
                #for link in soup.findAll('loc'):
                #    urls.append(link.text)
                for link in soup.find_all('a'):
                    urls.append(link.get('href')) 
                print("URLs so far: ", len(urls))
        else:
            for i in range(1, 29):
                if i <10:
                    daynum = "0" + str(i)
                else:
                    daynum = str(i)
                url = "https://www.nytimes.com/sitemap" + ym + daynum + "/"
                print("Extracting from: ", url)
                reqs = requests.get(url, headers=headers)
                soup = BeautifulSoup(reqs.text, 'html.parser')
                #for link in soup.findAll('loc'):
                #    urls.append(link.text)
                for link in soup.find_all('a'):
                    urls.append(link.get('href')) 
                print("URLs so far: ", len(urls))      


# STEP 1: Get rid or urls from blacklisted sources
blpatterns = ['/style/', '/sports/', '/health/', '/dining/', '/travel/', '/realestate/', '/books/', '/arts/', '/video/', '/slideshow/', '/blog//learning/', '/topic/', '/auth/', '/section/', '/packages/', '/interactive/', '/column/', '/sitemap/', '/well/', '/results/', '//funnytimes.com/', '/opinion/', '/fashion/', '/quotation-of-the-day', '/pageoneplus/', '/video/', '/crosswords/', '/magazine/', '/movies/', '/issue/', '/live/', '/help.nytimes', 'pgtype=Homepage']
clean_urls = []
for url in urls:
    if "nytimes.com" in url:
        count_patterns = 0
        for pattern in blpatterns:
            if pattern in url:
                count_patterns = count_patterns + 1
        if count_patterns == 0:
            clean_urls.append(url)

# List of unique urls:
list_urls = list(set(clean_urls))

# Manually check urls:
#list_urls = list(set(urls))
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
            if "nytimes.com" in url:
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
                        #print(article['title'][0:100])
                        #print(article['maintext'][0:100])
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
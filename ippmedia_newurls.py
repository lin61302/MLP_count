#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Oct 13 2021

@author: diegoromero

This script obtains urls from keyword searches and inserts
them in the db (not in the 'direct-urls' collection).
"""

# Packages:
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
source = 'ippmedia.com'

## STEP 1: URLS FROM TOPICS

## TOPICS 
#keywords = ['elections']
#endnumber = ['3']
keywords = ['elections','judge','National%20Assembly','president','protest','riot','journalist','conflict','police','activist','law','minister','opposition','military','ban','media','press','legislation']
endnumber = ['148','65','168','1028','38','7','48','150','345','38','411','983','186','118','114','412','176','77']

for word in keywords:
    indexword = keywords.index(word)
    endnumberx = endnumber[indexword]

    for i in range(0, int(endnumberx)):
        if i == 0:
            url = "https://www.ippmedia.com/en/search/node/" + word + "%20language%3Aen%2Cund"
        else:
            url = "https://www.ippmedia.com/en/search/node/" + word + "%20language%3Aen%2Cund?page=" + str(i) 
        
        print(url)

        reqs = requests.get(url, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        for link in soup.find_all('a'):
            urls.append(link.get('href')) 

# 2. URLS from SECTIONS:
#url format example: https://www.ippmedia.com/en/news?page=7
keywords = ['news']
endnumber = ['3289']

for word in keywords:
    indexword = keywords.index(word)
    endnumberx = endnumber[indexword]

    for i in range(1, int(endnumberx)):
        if i == 1:
            url = "https://www.ippmedia.com/en/" + word 
        else:
            url = "https://www.ippmedia.com/en/" + word + "?page=" + str(i) 
        
        print(url)

        reqs = requests.get(url, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        for link in soup.find_all('a'):
            urls.append(link.get('href')) 


# List of unique urls, after deleting articles from blacklisted sections:
blpatterns = ['/michezo/', '/reporters/', '/tag/', '/editorial/', '/wp-content/', '/columnist/', '/media/', '/function.', '/javascript/', '/frontend/']

clean_urls = []
for url in urls:
    count_patterns = 0
    for pattern in blpatterns:
        if pattern in url:
            count_patterns = count_patterns + 1
    if count_patterns == 0:
        clean_urls.append(url)

list_urls = list(set(clean_urls))

print("Total number of useful urls found: ", len(list_urls))


#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Dropbox/ferloo_test.csv')  


## INSERTING URLS IN THE 'direct-urls' collection

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
                        if "ippmedia.com" in url:
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
                                #article['source_domain'] = source

                                #print(article['date_publish'])
                                # Fixing Date
                                soup = BeautifulSoup(response.content, 'html.parser')
                                try:
                                    contains_date = soup.find("meta", {"name":"shareaholic:article_published_time"})
                                    date_p = contains_date['content']
                                    article_date = dateparser.parse(date_p,date_formats=['%d/%m/%Y'])
                                    article['date_publish'] = article_date
                                except:
                                    article_date = article['date_publish']
                                    article['date_publish'] = article_date

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
                                    #print(article['title'])
                                    #print(article['maintext'])
                                    db[colname].insert_one(article)
                                    print("Inserted! in ", colname)
                                    # count:
                                    url_count = url_count + 1
                                    db['urls'].insert_one({'url': article['url']})
                                except DuplicateKeyError:
                                    print("DUPLICATE! Not inserted.")
                            except Exception as err: 
                                print("ERRORRRR......", err)
                                pass
                        else:
                            pass



print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")

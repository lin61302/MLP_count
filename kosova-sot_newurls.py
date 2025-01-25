#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Oct 26 2021

@author: diegoromero

This script obtains urls from the sitemap of the source and also from 
specific sections, and inserts them in the db.
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

# seed the pseudorandom number generator
from random import seed
from random import random
from random import randint
# seed random number generator
seed(1)


# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

# headers for scraping
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


## COLLECTING URLS
urls = []

## NEED TO DEFINE SOURCE!
source = 'kosova-sot.info'

## STEP 1: URLS FROM TOPICS

## TOPICS 
keywords = ['president','zgjedhjeve','parlament','proteste','opozite','gazetar','policia','aktivist','arrest']
endnumber = ['13','14','14','14','14','14','14','14','14']
#randomkey = ['f2a9ad6922eeb29eaed63b16bb54c68b7295238f','58bc4f1802365eedf994fd452aafcb37124b759e','75a2dcb122eb32cceff1067f0476e58cd3f9f9cf','1577b338cddbdff79f4b946773f96e758b8cd2cb','']

randomkeybit1 = ['f2a9ad6922','eeb29eaed6','3b16bb54c6','8b7295238f','58bc4f1802','365eedf994','fd452aafcb','37124b759e','75a2dcb122','eb32cceff1','067f0476e5','8cd3f9f9cf','9ce2ba28ca','1b3d31da6f','a2e64b548f','92fbfc660a','00383cf60f','a8c7951387','5e29529ae2','0052d5ee86']

randomkeybit2 = ['1577b338cd','dbdff79f4b','946773f96e','758b8cd2cb','3f40dde4d7','1e02c76026','ae3cfb6e04','05be6eb679','a32a873132','e63a2ff7d0','6ae598e4c8','740bc337ba','35742cf62e','a3daba70af','bcff87729a','e1c3039ec2','4cbaae14ff','da5aaaf0e0','d6f9477ce8','0220fe5e75']

randomkeybit3 = ['9a1e0a5329','96c90ff99a','8d1e48f076','babb7971e9','624e5a5ebf','dc7d4056a2','6ee41b86a8','65148be214','c04fb46cf0','2d7bca69fe','78917fd55c','7b30671ccb','d8cf8bdee3','e0d8df3029','cbed68f044','ebf48e8500','caf64ab5e1','dd6740c307','ff78b198fe','1f2ddec701']

randomkeybit4 = ['26c0e528a6','0177c11005','ccad57a213','f86b3fcfb3','ae11911877','9054785c35','4f8bc53c9c','d187cb1011','ce5c3da7fc','ec37f9cbe5','16c675e6dc','09f36700bb','e6df4775aa','84c54a19f2','0e86b2447f','7c03b47dc9','9f845df31d','73adfd1cc0','46f5d5f78d','01245ee704']


for word in keywords:
    indexword = keywords.index(word)
    endnumberx = endnumber[indexword]
    value1 = randint(0, 19)
    value2 = randint(0, 19)
    value3 = randint(0, 19)
    value4 = randint(0, 19)
    randomkey = randomkeybit1[value1] + randomkeybit2[value2] + randomkeybit3[value3] + randomkeybit4[value4]

    for i in range(1, int(endnumberx)):
        if i == 1:
            url = "https://www.kosova-sot.info/kerko/?keyword=" + word
        else:
            url = "https://www.kosova-sot.info/kerko/?keyword=" + word + "&formkey=" + randomkey + "&faqe=" + str(i)
    
        print(url)

        reqs = requests.get(url, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        for link in soup.find_all('a'):
            urls.append(link.get('href')) 

        print("Number of urls so far: ", len(urls))

# List of unique urls:
list_urls = list(set(urls))

print("Total number of urls found: ", len(list_urls))


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
            if "/opinione/" in url:
                pass
            else:
                if "/cdn-cgi/" in url:
                    pass
                else:
                    if "/auto-tech/" in url:
                        pass
                    else:
                        if "kosova-sot.info" in url:
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
                                # insert into the db

                                ## Fixing may text
                                soup = BeautifulSoup(response.content, 'html.parser')
        
                                try:
                                    maintext = soup.find('div', {'class': 'news-content'}).text.strip()
                                    article['maintext'] = maintext

                                except: 
                                    try:
                                        soup.find('div', {'class': 'news-content'}).find_all('p')
                                        maintext = ''
                                        for i in soup.find('div', {'class': 'news-content'}).find_all('p'):
                                            maintext += i.text.strip()
                                        article['maintext'] = maintext
                                    except:
                                        try:
                                            soup.find('div', {'class' : 'left-side-news'}).find_all('p')
                                            maintext = ''
                                            for i in soup.find('div', {'class' : 'left-side-news'}).find_all('p'):
                                                maintext += i.text.strip()
                                            article['maintext'] = maintext
                                        except:
                                            maintext = None
                                            article['maintext']  = maintext
                                print("newsplease maintext: ", article['maintext'][:50])


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
                            url = "https://www.kosova-sot.info" + url
                            print(url, "FIXED")
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


print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")

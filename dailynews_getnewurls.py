#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Oct 25 2021

@author: diegoromero

This script obtains urls from the sitemap of the source and also from 
specific sections, and inserts them in the 'direct-urls' collection.
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
import random
from random import randint, randrange
from warnings import warn
import json
from pymongo import MongoClient
from urllib.parse import urlparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound
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
source = 'dailynews.co.tz'

## STEP 1: URLS FROM TOPICS

## TOPICS 
keywords = ['local','business','features']
endnumber = ['185','40','140']

for word in keywords:
    indexword = keywords.index(word)
    endnumberx = endnumber[indexword]

    for i in range(0, int(endnumberx)):
        if i == 0:
            url = "https://dailynews.co.tz/tags/" + word 
            #+ ".aspx"   
        else:
            numberx = i * 10
            url = "https://dailynews.co.tz/tags/" + word + "/" + str(numberx) 
        
        print(url)

        reqs = requests.get(url, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        for link in soup.find_all('a'):
            urls.append(link.get('href')) 


# List of unique urls:
list_urlsb = []
for url in urls:
    if "/2021-" in url:
        list_urlsb.append(url)


list_urls = list(set(list_urlsb))

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
                #print("newsplease date: ", article['date_publish'])

                ## Fixing Date:
                soup = BeautifulSoup(response.content, 'html.parser')

                try:
                    contains_date = soup.find("div", {"class":"post-meta-date"}).text
                    #contains_date = soup.find("i", {"class":"fa fa-calendar"}).text
                    article_date = dateparser.parse(contains_date, date_formats=['%d/%m/%Y'])
                    article['date_publish'] = article_date
                except:
                    article_date = article['date_publish']
                    article['date_publish'] = article_date
                
                ## Fixing Title:
                try:
                    contains_article = soup.find("meta", {"property":"og:title"})
                    article_title = contains_article['content']
                    article['title']  = article_title   
                except:
                    article_title  = article['title']
                    article['title'] = article_title

                # Get Main Text:
                try: 
                    contains_text = soup.findAll("span", {"style":"font-size:16px"})
                    maintext = contains_text[0].text + " " + contains_text[1].text + " " + contains_text[2].text + " " + contains_text[3].text  
                    article['maintext'] = maintext
                except:
                    maintext = article['maintext']
                    article['maintext'] = maintext
                #print(article['title'])
                #print("fixed date: ", article['date_publish'])
                #print(article['maintext'])
                ## Inserting into the db
                try:
                    year = article['date_publish'].year
                    month = article['date_publish'].month
                    colname = f'articles-{year}-{month}'
                    #print(article)
                except:
                    colname = 'articles-nodate'
                #print("Collection: ", colname)
                try:
                    #TEMP: deleting the stuff i included with the wrong domain:
                    #myquery = { "url": final_url, "source_domain" : 'web.archive.org'}
                    #db[colname].delete_one(myquery)
                    # Inserting article into the db:
                    db[colname].insert_one(article)
                    # count:
                    url_count = url_count + 1
                    print("Inserted! in ", colname, " - number of urls so far: ", url_count)
                    db['urls'].insert_one({'url': article['url']})
                except DuplicateKeyError:
                    print("DUPLICATE! Not inserted.")
            except Exception as err: 
                print("ERRORRRR......", err)
                pass


print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")
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
#from time import time
#from time import sleep
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
#headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}

#headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15 Version/13.0.4'}
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

#headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'}

## COLLECTING URLS
urls = []

## NEED TO DEFINE SOURCE!
source = 'france24.com'

## STEP 0: Define dates
languages = ["fr", "en", "es"]

yearn = "2021"
monthn = 10

months_en = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
months_fr = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre"]
months_es = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

daystart = "1"
dayend = "31" 

#https://www.france24.com/en/archives/2021/10/29-October-2021
#https://www.france24.com/fr/archives/2021/11/15-novembre-2021
#https://www.france24.com/es/archivos/2021/11/15-noviembre-2021

repeat_urls = []
for language in languages:
    if language == "en":
        month_name = months_en[monthn-1]
    else:
        if language == "fr":
            month_name = months_fr[monthn-1]
        else:
            month_name = months_es[monthn-1]

    for i in range(int(daystart), int(dayend)+1):
        # Month
        if monthn <10:
            monthstr = "0" + str(monthn)
        else:
            monthstr = str(monthn)

        # Day
        if i <10:
            daynum = "0" + str(i)
        else:
            daynum = str(i)
        url = "https://www.france24.com/" + language + "/archives/" + yearn + "/" + monthstr + "/" + daynum + "-" + month_name + "-" + yearn
        print("Extracting from: ", url)
        time.sleep(12)
        reqs = requests.get(url, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        print(soup)
        #for link in soup.findAll('loc'):
        #    urls.append(link.text)
        smapurls = []
        for link in soup.find_all('a'):
            urls.append(link.get('href')) 
            smapurls.append(link.get('href')) 
        print("URLs so far: ", len(urls))
        if len(smapurls) == 0:
            repeat_urls.append(url)

for url in repeat_urls:
    print("Extracting from: ", url)
    time.sleep(12)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    for link in soup.find_all('a'):
        urls.append(link.get('href')) 
    print("URLs so far: ", len(urls))


# STEP 1: Get rid or urls from blacklisted sources
blpatterns = ['/sport/', '/tv-shows/', '/culture/', '/archives/', '/voyage/', '/cultura/', '/deportes/', '/video/', '/vidéo/']

clean_urls = []
for url in urls:
    if url == "":
        pass
    else:
        if url == None:
            pass
        else:
            try: 
                if url.index("/") == 0:
                    count_patterns = 0
                    for pattern in blpatterns:
                        if pattern in url:
                            count_patterns = count_patterns + 1
                    if count_patterns == 0:
                        newurl = "https://www.france24.com" + url 
                        clean_urls.append(newurl)
            except:
                pass

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
            if "france24.com" in url:
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
                    
                    ## Fixing what needs to be fixed:
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # TITLE:
                    if article['title'] == None:
                        try:
                            contains_title = soup.find("meta", {"property":"og:title"})
                            article_title = contains_title["content"]
                            article['title'] = article_title
                        except:
                            try:
                                article_title = soup.find('title').text
                                article['title'] = article_title
                            except:
                                article['title'] == None
                    # MAIN TEXT:
                    if article['maintext'] == None:
                        try:
                            contains_text = soup.find("meta", {"name":"description"})
                            article_text = contains_text["content"]
                            article['maintext'] = article_text
                        except:
                            article['maintext'] = None

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


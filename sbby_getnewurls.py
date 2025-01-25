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
# from peacemachine.helpers import urlFilter
from newsplease import NewsPlease
from dotenv import load_dotenv

# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

base_list = []
base = ['https://www.sb.by/articles/main_Incidents/?PAGEN_2=',\
        'https://www.sb.by/articles/main_world/?PAGEN_2=',\
        'https://www.sb.by/articles/tags/%D0%9E%D0%BA%D0%BD%D0%BE%20%D0%B2%20%D0%9A%D0%B8%D1%82%D0%B0%D0%B9/?PAGEN_2=',\
        'https://www.sb.by/articles/main_economy/?PAGEN_2=', \
        'https://www.sb.by/articles/main_society/?PAGEN_2=', \
        'https://www.sb.by/articles/main_policy/?PAGEN_2=']
# main_Incidents, accidents and society, international news

hdr = {'User-Agent': 'Mozilla/5.0'} #header settings
# months that need more urls
for b in base:
    for i in range(1, 41):
        base_list.append(b + str(i))

print(len(base_list))

direct_URLs = []
for b in base_list:
    print(b)
    try: 
        hdr = {'User-Agent': 'Mozilla/5.0'}
        req = requests.get(b, headers = hdr)
        soup = BeautifulSoup(req.content)
        
        item = soup.find_all('div', {'class' : 'media-old'})
        for i in item:
            direct_URLs.append(i.find('a', href = True)['href'])
        print(len(direct_URLs))
    except:
        pass

print(direct_URLs[:5])
direct_URLs =list(set(direct_URLs))

print(len(direct_URLs))

final_result = ['https://www.sb.by' + i for i in direct_URLs]
print(final_result[:5])

source = 'sb.by'
url_count = 0
for url in final_result:
    if url:
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
            article['source_domain'] = source
            #print("newsplease date: ", article['date_publish'])

            ## Fixing Date:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Get Title: 
            try:
                article_title = soup.find('meta', property = 'og:title')['content']
                article['title']  = article_title  
            except:
                try:
                    article_title = soup.find('title').text
                    article['title']  = article_title  
                except:
                    try:
                        article_title = soup.find('h1').text
                        article['title']  = article_title
                    except:
                        article['title']  = article['title'] 
            if article['title']:
                print(article['title'])

            # Get Main Text:
            try:
                maintext = soup.find('div', {'itemprop': 'articleBody'}).text
                article['maintext'] = maintext
            except: 
                try:
                    maintext = soup.find('div', {'class': 'block-text'}).text
                    article['maintext'] = maintext
                except:
                    article['maintext'] = article['maintext']
            if article['maintext']:
                print(article['maintext'][:50])
        

            # Get Date
            try:
                date = soup.find('meta', property="article:published_time")['content']
                article['date_publish'] = dateparser.parse(date).replace(tzinfo=None)
            except:
                try:
                    date = soup.find('time').text
                    article['date_publish'] = dateparser.parse(date, settings={'DATE_ORDER': 'DMY'})
                except:
                    article['date_publish'] = article['date_publish']
                   
            if article['date_publish']:
                print(article['date_publish'])


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
    else:
        pass

print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")

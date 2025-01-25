#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 11 2021

@author: diegoromero

This script updates csmonitor.com using daily sitemaps AND keyword searches,
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

import math
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
#hdr = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

## COLLECTING URLS
urls = []

## NEED TO DEFINE SOURCE!
source = 'csmonitor.com'

## Step 0: define starting month and year:

year_up = 2021
month_up = 11

## STEP 1: COLLECTING URLS FROM KEYWORD SEARCHES:
# keywords:
keywords = ['Afghanistan','Albania','Algeria','Andorra','Angola','Antigua+and+Barbuda','Argentina','Armenia','Australia','Austria','Azerbaijan','Bahamas','Bahrain','Bangladesh','Barbados','Belarus','Belgium','Belize','Benin','Bhutan','Bolivia','Bosnia+and+Herzegovina','Botswana','Brazil','Brunei','Bulgaria','Burkina+Faso','Burundi','Côte+d%27Ivoire','Cabo+Verde','Cambodia','Cameroon','Canada','Central+African+Republic','Chad','Chile','China','Colombia','Comoros','Congo','Costa+Rica','Croatia','Cuba','Cyprus','Czechia','Czech+Republic','Denmark','Djibouti','Dominica','Dominican+Republic','Ecuador','Egypt','El+Salvador','Equatorial+Guinea','Eritrea','Estonia','Eswatini','Ethiopia','Fiji','Finland','France','Gabon','Gambia','Georgia','Germany','Ghana','Greece','Grenada','Guatemala','Guinea','Guinea-Bissau','Guyana','Haiti','Vatican','Honduras','Hungary','Iceland','India','Indonesia','Iran','Iraq','Ireland','Israel','Italy','Jamaica','Japan','Jordan','Kazakhstan','Kenya','Kiribati','Kuwait','Kyrgyzstan','Laos','Latvia','Lebanon','Lesotho','Liberia','Libya','Liechtenstein','Lithuania','Luxembourg','Madagascar','Malawi','Malaysia','Maldives','Mali','Malta','Marshall+Islands','Mauritania','Mauritius','Mexico','Micronesia','Moldova','Monaco','Mongolia','Montenegro','Morocco','Mozambique','Myanmar','Namibia','Nauru','Nepal','Netherlands','New+Zealand','Nicaragua','Niger','Nigeria','North+Korea','North+Macedonia','Norway','Oman','Pakistan','Palau','Palestine','Panama','Papua+New+Guinea','Paraguay','Peru','Philippines','Poland','Portugal','Qatar','Romania','Russia','Rwanda','Saint+Kitts+and+Nevis','Saint+Lucia','Samoa','San+Marino','Sao+Tome+and+Principe','Saudi+Arabia','Senegal','Serbia','Seychelles','Sierra+Leone','Singapore','Slovakia','Slovenia','Solomon+Islands','Somalia','South+Africa','South+Korea','South+Sudan','Spain','Sri+Lanka','Sudan','Suriname','Sweden','Switzerland','Syria','Tajikistan','Tanzania','Thailand','Timor-Leste','Togo','Tonga','Trinidad+and+Tobago','Tunisia','Turkey','Turkmenistan','Tuvalu','Uganda','Ukraine','United+Arab+Emirates','UAE','United+Kingdom','Uruguay','Uzbekistan','Vanuatu','Venezuela','Vietnam','Yemen','Zambia','Zimbabwe','coup','arrest','journalist','press','press+freedom','activist','military','police','law']
#keywords = ['Guatemala','El+Salvador']
#https://www.csmonitor.com/content/search?SearchText=Egypt&SearchSectionID=-1&SearchDate=-1&sort=
#https://www.csmonitor.com/content/search/(offset)/30?SearchText=Egypt&SearchSectionID=-1&SearchDate=-1&sort=

for word in keywords:
    initial_url = "https://www.csmonitor.com/content/search?SearchText=" + word + "&SearchSectionID=-1&SearchDate=-1&sort="
    
    req = requests.get(initial_url, headers = headers)
    soup = BeautifulSoup(req.content, 'html.parser')

    article_title = soup.find("h2").text
    vectorwords = article_title.split()
    articlenumber = vectorwords[len(vectorwords)-2] 
    number_pages = float(articlenumber)/10
    number_pages = math.ceil(number_pages)

    print(number_pages, " pages about ", word)

    if number_pages > 100:
        end_page = 20
    else:
        if number_pages > 20:
            end_page = 10
        else:
            end_page = number_pages
    
    # URLs from first page:
    soup = BeautifulSoup(req.text, 'html.parser')
    #for link in soup.findAll('loc'):
    #    urls.append(link.text)
    for link in soup.find_all('a'):
        urls.append(link.get('href')) 
    print("URLs so far: ", len(urls))

    for i in range(1,end_page):
        offsetpage = i*10
        url = "https://www.csmonitor.com/content/search/(offset)/" + str(offsetpage) + "?SearchText=" + word + "&SearchSectionID=-1&SearchDate=-1&sort="
        print("+ + + Extracting from: ", url)
        reqs = requests.get(url, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        #for link in soup.findAll('loc'):
        #    urls.append(link.text)
        for link in soup.find_all('a'):
            urls.append(link.get('href')) 
        print("URLs so far: ", len(urls))

# Manually check urls:
#list_urls = list(set(urls))
#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Downloads/test_CSM.csv')  

# STEP 2: Get rid or urls from blacklisted sources
blpatterns = ['/Books/', '/The-Culture/', '/The-Culture', '/Podcasts/', '/Photo-Galleries/', '/Olympics/', '/Olympics/', '/Commentary/', '/Sports/', '/layout/set/text/World/', '/(page)/', '/About/', '/People/', '/Daily/', '/Commentary/', '/search/', '/Science/', '/Technology/', '/login.']
clean_urls = []
for url in urls:
    if url == "":
        pass
    else:
        if url == None:
            pass
        else:
            if "https://" in url:
                pass
            else:
                count_patterns = 0
                for pattern in blpatterns:
                    if pattern in url:
                        count_patterns = count_patterns + 1
                if count_patterns == 0:
                    new_url = "https://www.csmonitor.com" + url
                    clean_urls.append(new_url)

# List of unique urls:
list_urls = list(set(clean_urls))

# Manually check urls:
#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Downloads/test_csm.csv')  

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
            if "csmonitor.com" in url:
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
                        #db[colname].insert_one(article)
                        # OLD:
                        #if article['date_publish'].month >= month_up:
                        #    if article['date_publish'].year >= year_up:
                        #        url_count = url_count + 1
                                # Inserting article into the db:
                        #        db[colname].insert_one(article)
                        #        print("Inserted! in ", colname, " - number of urls so far: ", url_count)
                        #        db['urls'].insert_one({'url': article['url']})
                        #    else: 
                        #        print("Not from ", year_up)
                        #else:
                        #    print("Not within the month range.")
                        # NEW:
                        if article['date_publish'].year == year_up:
                            if article['date_publish'].month >= month_up:
                                url_count = url_count + 1
                                # Inserting article into the db:
                                db[colname].insert_one(article)
                                print("Inserted! in ", colname, " - number of urls so far: ", url_count)
                                db['urls'].insert_one({'url': article['url']})
                            else:
                                print("Wrong month")
                        else:
                            if article['date_publish'].year > year_up:
                                url_count = url_count + 1
                                # Inserting article into the db:
                                db[colname].insert_one(article)
                                print("Inserted! in ", colname, " - number of urls so far: ", url_count)
                                db['urls'].insert_one({'url': article['url']})
                            else: 
                                print("From before ", year_up) 
                    except DuplicateKeyError:
                        print("DUPLICATE! Not inserted.")
                except Exception as err: 
                    print("ERRORRRR......", err)
                    pass
            else:
                pass


print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")
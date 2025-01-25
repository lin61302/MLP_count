#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 11 2021

@author: diegoromero

This script completes bbc.com using keyword queries
It can be run whenever.
 
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
siteurls = []

## NEED TO DEFINE SOURCE!
source = 'bbc.com'

## STEP 0: COLLECTING URLS FROM KEYWORD SEARCHES:
# keywords:
countries = ['Afghanistan','Albania','Algeria','Andorra','Angola','Antigua+and+Barbuda','Argentina','Armenia','Australia','Austria','Azerbaijan','Bahamas','Bahrain','Bangladesh','Barbados','Belarus','Belgium','Belize','Benin','Bhutan','Bolivia','Bosnia+and+Herzegovina','Botswana','Brazil','Brunei','Bulgaria','Burkina+Faso','Burundi','Ivory+Coast','Cabo+Verde','Cambodia','Cameroon','Canada','Central+African+Republic','Chad','Chile','China','Colombia','Comoros','Congo','Costa+Rica','Croatia','Cuba','Cyprus','Czechia','Czech+Republic','Denmark','Djibouti','Dominica','Dominican+Republic','Ecuador','Egypt','El+Salvador','Equatorial+Guinea','Eritrea','Estonia','Eswatini','Ethiopia','Fiji','Finland','France','Gabon','Gambia','Georgia','Germany','Ghana','Greece','Grenada','Guatemala','Guinea','Guinea-Bissau','Guyana','Haiti','Vatican','Honduras','Hungary','Iceland','India','Indonesia','Iran','Iraq','Ireland','Israel','Italy','Jamaica','Japan','Jordan','Kazakhstan','Kenya','Kiribati','Kuwait','Kyrgyzstan','Laos','Latvia','Lebanon','Lesotho','Liberia','Libya','Liechtenstein','Lithuania','Luxembourg','Madagascar','Malawi','Malaysia','Maldives','Mali','Malta','Marshall+Islands','Mauritania','Mauritius','Mexico','Micronesia','Moldova','Monaco','Mongolia','Montenegro','Morocco','Mozambique','Myanmar','Namibia','Nauru','Nepal','Netherlands','New+Zealand','Nicaragua','Niger','Nigeria','North+Korea','North+Macedonia','Norway','Oman','Pakistan','Palau','Palestine','Panama','Papua+New+Guinea','Paraguay','Peru','Philippines','Poland','Portugal','Qatar','Romania','Russia','Rwanda','Saint+Kitts+and+Nevis','Saint+Lucia','Samoa','San+Marino','Sao+Tome+and+Principe','Saudi+Arabia','Senegal','Serbia','Seychelles','Sierra+Leone','Singapore','Slovakia','Slovenia','Solomon+Islands','Somalia','South+Africa','South+Korea','South+Sudan','Spain','Sri+Lanka','Sudan','Suriname','Sweden','Switzerland','Syria','Tajikistan','Tanzania','Thailand','Timor-Leste','Togo','Tonga','Trinidad+and+Tobago','Tunisia','Turkey','Turkmenistan','Tuvalu','Uganda','Ukraine','United+Arab+Emirates','UAE','United+Kingdom','Uruguay','Uzbekistan','Vanuatu','Venezuela','Vietnam','Yemen','Zambia','Zimbabwe']
keywords = ['protest','riot','war','coup','arrest','journalist','press','press+freedom','activist','military','police','law','congress','president','prime+minister','cabinet','government','human+rights']

#countries = ['Guatemala','Barbados']
#keywords = ['government','human+rights']

# https://www.bbc.co.uk/search?q=Guatemala+human+rights&page=1

for country in countries:
    for word in keywords:
        urls = list(set(urls))
        indexword = keywords.index(word)
        endnumberx = 29

        for i in range(1, int(endnumberx)+1):
            url = "https://www.bbc.co.uk/search?q=" + country + "+" + word + "&page=" + str(i)

            print("Keyword: ", word, " -> URL: ", url)

            reqs = requests.get(url, headers=headers)
            soup = BeautifulSoup(reqs.text, 'html.parser')

            for link in soup.find_all('a'):
                urls.append(link.get('href')) 
            print("URLs so far: ", len(urls))

# JUST KEYWORDS:
keywords = ['protest','riot','war','coup','arrest','journalist','press','press+freedom','activist','military','police','law','congress','president','prime+minister','cabinet','government','human+rights']

for word in keywords:
    indexword = keywords.index(word)
    endnumberx = 29

    for i in range(1, int(endnumberx)+1):
        url = "https://www.bbc.co.uk/search?q=" + word + "&page=" + str(i)

        print("Keyword: ", word, " -> URL: ", url)

        reqs = requests.get(url, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        for link in soup.find_all('a'):
            urls.append(link.get('href')) 
        print("URLs so far: ", len(urls))


# STEP 2: Get rid or urls from blacklisted sources
blpatterns = ['/sport/', '/travel/', '/culture/', '/sport-', '/multimedia/', '/learningenglish/', '/bbc_arabic_radio/', '/arts/', '/naidheachdan/', '/av/', '/programmes/','/usingthebbc/','/search?q=']

clean_urls = []
for url in urls:
    if url == "":
        pass
    else:
        if url == None:
            pass
        else:
            if "bbc.co.uk" in url:
                count_patterns = 0
                for pattern in blpatterns:
                    if pattern in url:
                        count_patterns = count_patterns + 1
                if count_patterns == 0:
                    clean_urls.append(url)
            else:
                if "bbc.com" in url:
                    count_patterns = 0
                    for pattern in blpatterns:
                        if pattern in url:
                            count_patterns = count_patterns + 1
                    if count_patterns == 0:
                        clean_urls.append(url)
                else:
                    count_patterns = 0
                    for pattern in blpatterns:
                        if pattern in url:
                            count_patterns = count_patterns + 1
                    if count_patterns == 0:
                        newurl = "https://www.bbc.com" + url 
                        clean_urls.append(newurl)
            
# List of unique urls:
list_urls = list(set(clean_urls))
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
                    monthx = 9
                    yearx = 2021
                    if colname != 'articles-nodate':
                        if article['date_publish'].year == yearx:
                            if int(article['date_publish'].month) >= monthx:
                                # Inserting article into the db:
                                db[colname].insert_one(article)
                                # count:
                                url_count = url_count + 1
                                print(article['date_publish'], " MONTH: ",article['date_publish'].month)
                                print(article['title'][0:100])
                                print(article['maintext'][0:200])
                                print("Inserted! in ", colname, " - number of urls so far: ", url_count)
                                db['urls'].insert_one({'url': article['url']})
                            else:
                                pass
                        else:
                            pass
                    else:
                        print('articles-nodate -- Not inserted.')
                except DuplicateKeyError:
                    print("DUPLICATE! Not inserted.")
            except Exception as err: 
                print("ERRORRRR......", err)
                pass



print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")
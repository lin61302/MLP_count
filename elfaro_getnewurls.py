#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 1 2021

@author: diegoromero

This script updates elfaro.net using keyword queries.
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
source = 'elfaro.net'

#https://elfaro.net/es/archivo?f_search_keywords=activista&tpl=84&ls-src0=&f_search_mode=1&f_search_level=1&f_search_scope=title&f_search_articles=Buscar
#https://elfaro.net/es/archivo?tpl=84&ls-src0=&f_search_mode=1&f_search_level=1&f_search_scope=title&type_not=Cover&f_search_keywords=activista&f_search_articles=Buscar
#https://elfaro.net/es/archivo?f_search_keywords=corrupci%C3%B3n&tpl=84&ls-src0=&f_search_mode=1&f_search_level=1&f_search_scope=title&f_search_articles=Buscar
#https://elfaro.net/es/archivo?f_search_keywords=campa%C3%B1a&tpl=84&ls-src0=&f_search_mode=1&f_search_level=1&f_search_scope=title&f_search_articles=Buscar
#https://elfaro.net/es/archivo?tpl=84&ls-src0=5&f_search_level=1&f_search_articles=Buscar&f_search_keywords=corrupci%C3%B3n&f_search_scope=title
#https://elfaro.net/es/archivo?tpl=84&ls-src0=10&f_search_level=1&f_search_articles=Buscar&f_search_keywords=corrupci%C3%B3n&f_search_scope=title
#https://elfaro.net/es/archivo?tpl=84&ls-src0=140&f_search_level=1&f_search_articles=Buscar&f_search_keywords=pol%C3%ADtica&f_search_scope=title
#https://elfaro.net/es/archivo?tpl=84&ls-src0=15&f_search_level=1&f_search_articles=Buscar&f_search_keywords=politica&f_search_scope=title
#https://elfaro.net/es/archivo?tpl=84&ls-src0=&f_search_mode=1&f_search_level=1&f_search_scope=title&type_not=Cover&f_search_keywords=partido+político&f_search_articles=Buscar

# DEFINE KEYWORDS
keywords = ['corrupción','política','partido+político','partidos+políticos','elecciones','presidente','congreso','candidato','candidata','candidatos','activista','activistas','periodista','periodistas','prensa','publicación','ley','reforma','cambio','El+Salvador','obra+pública','compras','protesta','protestas','manifestacion','manifestaciones','arresto','arrestado','arrestada','estudiantes','sindicatos','líder','líderes','oposición','soborno','cooperación','cumbre','reunión','diálogo','golpe+de+estado','golpe','violencia','militar','ejército','fuerzas+armadas','policía','policías','soldados','amenaza','caso','difamación','acusado','corte','tribunal','tribunales','juez','jueza','jueces','abogado','abogada','abogadas','pandilla','pandillas','pandillero','mara','marero','mareros','justicia','libertad','investigación','constitución','electoral','campaña','promesa','TSE','magistrado','magistrada','magistrados','FMLN','Arena','GANA','PCN','Cambio+Democrático','CD','Nuestro+Tiempo','vamos','Nuevas+Ideas','demócrata','democracia','paz','turba','manifestantes','migrantes','alcalde','alcaldes','congresista','diputado','centroamérica','derechos+humanos']

# WHEN UPDATING: Use endnumber to define the number of pages to scrape per keyword:
#endnumber = ['1','4']

# https://search.news.cn/?lang=en#search/0/biden/17/

for word in keywords:
    # Use these lines when you want to update this sources, to manually define number of pages:
    #indexword = keywords.index(word)
    #endnumberx = endnumber[indexword]

    # SCRAPING URLS:
    url = "https://elfaro.net/es/archivo?f_search_keywords=" + word + "&tpl=84&ls-src0=&f_search_mode=1&f_search_level=1&f_search_scope=title&f_search_articles=Buscar"
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    # WHEN UPDATING, COMMENT OUT ROWS: 96-102
    # Defining number of pages per keyword
    try: 
        variable = soup.find("span", {"class":"no-hidden"}).text
        indexs = variable.index("/") + 1
        indexl = len(variable)-1
        endnumberx = variable[indexs:indexl]
    except:
        endnumberx = "0"

    # Obtaining urls for each page: 
    if int(endnumberx) > 1:
        # urls from first page: 
        for link in soup.find_all('a'):
            urls.append(link.get('href')) 
        print("Keyword: ", word," -> Page: 1 /", endnumberx, " URLs so far: ", len(urls))
        # urls from the next pages:
        for i in range(1, int(endnumberx)+1):
            numbarts = i*5
            url = "https://elfaro.net/es/archivo?tpl=84&ls-src0=" + str(numbarts) + "&f_search_level=1&f_search_articles=Buscar&f_search_keywords=" + word + "&f_search_scope=title"

            reqs = requests.get(url, headers=headers)
            soup = BeautifulSoup(reqs.text, 'html.parser')

            for link in soup.find_all('a'):
                urls.append(link.get('href')) 
            print("Keyword: ", word," -> Page: ", str(i+1), "/", endnumberx, " URLs so far: ", len(urls))
    else:
        if int(endnumberx) == 1:
            for link in soup.find_all('a'):
                    urls.append(link.get('href')) 
            print("Keyword: ", word," -> Page: 1 /", endnumberx, " URLs so far: ", len(urls))
        else:
            print("No results for Keyword: ", word)

# Manually check urls:
#list_urls = list(set(urls))
#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Downloads/test.csv')  
#print("DONE")

# STEP 2: Get rid or urls from blacklisted sources
blpatterns = ['/columnas/', '/ef_foto/', '/latienda.', '/ef_academico/', '/ef_tv/', '/ef_radio/', '/cdn-cgi/', '/opinion/', '/fotos/', '/ef_academico/','f_search_scope=','?st-full_text=all&tpl=11','/efradio/','/academico/']
list_urls = []
dedups = list(set(urls))
for url in dedups :
    try:
        if "/es/" in url:
            count_patterns = 0
            for pattern in blpatterns:
                if pattern in url:
                    count_patterns = count_patterns + 1
            if count_patterns == 0:
                if "https://elfaro.net" in url:
                    if "whatsapp://send?text=" in url:
                        newurl = url[21:]
                        list_urls.append(newurl)
                    else:
                        if "https://www.facebook.com/sharer/sharer.php?u=" in url:
                            newurl = url[45:]
                            list_urls.append(newurl)
                        else:
                            list_urls.append(url)
                else:
                    if "http://www.elfaro.net" in url:
                        if "whatsapp://send?text=" in url:
                            newurl = url[21:]
                            list_urls.append(newurl)
                        else:
                            if "https://www.facebook.com/sharer/sharer.php?u=" in url:
                                newurl = url[45:]
                                list_urls.append(newurl)
                            else:
                                list_urls.append(url)
                    else:
                        newurl = "https://elfaro.net" + url
                        list_urls.append(newurl)
    except:
        pass

# List of unique urls:
list_urls = list(set(list_urls))
print("Total number of USABLE urls found: ", len(list_urls))

#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Downloads/test.csv') 
#print("DONE")

## INSERTING IN THE DB:
url_count = 0
for url in list_urls:
    if url == "":
        pass
    else:
        if url == None:
            pass
        else:
            if "elfaro.net" in url:
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
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Get Date
                    try: 
                        contains_date = soup.find("span", {"class":"hidden-sm hidden-sm hidden-md hidden-lg"}).text
                        article_date = dateparser.parse(contains_date,date_formats=['%d/%m/%Y'])
                        article['date_publish'] = article_date  
                    except:
                        #article_date = article['date_publish']
                        #article['date_publish'] = article_date
                        article['date_publish'] = None

                    # Get Title: 
                    try:
                        #article_title = soup.find("title").text
                        contains_title = soup.find("meta", {"property":"og:title"})
                        article_title = contains_title['content']
                        article['title']  = article_title   
                    except:
                        article_title = article['title']
                        article['title'] = article_title
        
                    # Get Main Text:
                    try:
                        maintext_contains = soup.find("meta", {"property":"og:description"})
                        maintext = maintext_contains['content']
                        article['maintext'] = maintext
                    except: 
                        maintext = article['maintext']
                        article['maintext'] = maintext

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
                        print("+ Obtained Date: ", article['date_publish'])
                        #print(article['date_publish'].month)
                        print("+ TITLE: ", article['title'][0:20]," - MAIN TEXT: ",article['maintext'][0:20])
                        #print(article['maintext'][0:150])
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
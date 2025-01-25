#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 26 2021

@author: diegoromero

This script updates kohsantepheapdaily.com.kh using daily archives.
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
#urls = []

## NEED TO DEFINE SOURCE!
source = 'kohsantepheapdaily.com.kh'

## STEP 0: Define dates
## years:
start_year = 2015
end_year = 2017

years = list(range(start_year, end_year+1))

## months:
start_month = 1
end_month = 12
months = list(range(start_month, end_month+1))

months31 = [1,3,5,7,8,10,12]
months30 = [4,6,9,11]

### TO FIX DATES:
## NUMBERS
number_km = ["០១", "០២", "០៣", "០៤", "០៥", "០៦", "០៧", "០៨", "០៩","១០","១១", "១២", "១៣", "១៤", "១៥", "១៦", "១៧", "១៨", "១៩", "២០","២១", "២២", "២៣", "២៤", "២៥", "២៦", "២៧", "២៨", "២៩", "៣០", "៣១"]
#number_n = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]
## MONTHS
month_km = ["មករា","កុម្ភៈ","មីនា","មេសា","ឧសភា","មិថុនា","កក្កដា","សីហា","កញ្ញា","តុលា","វិច្ឆិកា","ធ្នូ"]
#month_n = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
## YEARS:
year_km = ["២០១២", "២០១៣", "២០១៤", "២០១៥", "២០១៦", "២០១៧", "២០១៨", "២០១៩", "២០២០","២០២១", "២០២២", "២០២៣", "២០២៤", "២០២៥", "២០២៦"]
year_n = ["2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"]


for year in years:
    yearstr = str(year)
    for month in months:
        # Month
        if month <10:
            monthstr = "0" + str(month)
        else:
            monthstr = str(month)
        # defining number of days:
        if month in months31:
            days = list(range(1, 32))
        else:
            if month in months30:
                days = list(range(1, 31))
            else:
                days = list(range(1, 29))
        # urls for the day:
        urls = []
        for day in days:
            # Day
            if day <10:
                daystr = "0" + str(day)
            else:
                daystr = str(day)
            # Mainpage:
            urlmain = "https://kohsantepheapdaily.com.kh/" + yearstr + "/" + monthstr + "/" + daystr
            print("Extracting from (main url): ", urlmain)
            #time.sleep(12)
            reqs = requests.get(urlmain, headers=headers)
            soup = BeautifulSoup(reqs.text, 'html.parser')
            for link in soup.find_all('a'):
                urls.append(link.get('href')) 
            print("URLs so far: ", len(urls))
            # Checking for extra pages:
            numbers = []
            for a in soup.findAll("a", {"class":"page-numbers"}):
                try:
                    numberx = int(a.text)
                except:
                    numberx = "not"
                #print(isinstance(numberx, int))
                if isinstance(numberx, int) == True:
                    numbers.append(numberx)
            if len(numbers) > 0:
                maxnumb = max(numbers)
                # Obtaining URLs from each page:
                for i in range(2,maxnumb+1):
                    url = "https://kohsantepheapdaily.com.kh/" + yearstr + "/" + monthstr + "/" + daystr + "/page/" + str(i)
                    print("Extracting from: ", url)
                    #time.sleep(12)
                    reqs = requests.get(url, headers=headers)
                    soup = BeautifulSoup(reqs.text, 'html.parser')
                    for link in soup.find_all('a'):
                        urls.append(link.get('href')) 
                    print("URLs so far: ", len(urls))
            else:
                pass
            
            ### PREPARING THE DAY'S URLS:
            # List of unique urls:
            dedup = list(set(urls))

            list_urls = []
            for url in dedup:
                if "/article/" in url:
                    list_urls.append(url)

            print("Total number of USABLE urls found: ", len(list_urls), " -- MONTH: ", monthstr, " DAY: ", daystr)

            ### SCRAPING THE DAY'S URLS
            ## INSERTING IN THE DB:
            url_count = 0
            for url in list_urls:
                if url == "":
                    pass
                else:
                    if url == None:
                        pass
                    else:
                        if "kohsantepheapdaily.com.kh" in url:
                            print(url, "FINE")
                            #time.sleep(2)
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
                                article['language']= "km"
                            
                                ## FIXING DATE:
                                soup = BeautifulSoup(response.text, 'html.parser')
                                #print(soup.find("time").text)
                                
                                
                                # DATE FROM THE PAGE OF THE ARCHIVE:
                                article_date = datetime(int(year),int(month),int(day))
                                #article_date = article['date_publish']
                                article['date_publish'] = article_date
                                #print("+ DATE ARCHIVE: ", article['date_publish'])
                                
                                ## FIXING MAIN TEXT:
                                try:
                                    contains_text = soup.find("meta", {"name":"description"})
                                    maintext = contains_text['content']
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
                                    #print(article['date_publish'].month)
                                    print(article['title'][0:100])
                                    print(article['maintext'][0:100])
                                    print(article['date_publish'])
                                    print("Inserted! in ", colname, " - number of urls so far: ", url_count, " OF: ", len(list_urls), " -- MONTH: ", monthstr, " DAY: ", daystr)
                                    db['urls'].insert_one({'url': article['url']})
                                except DuplicateKeyError:
                                    print("DUPLICATE! Not inserted.")
                            except Exception as err: 
                                print("ERRORRRR......", err)
                                pass
                        else:
                            pass


print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")
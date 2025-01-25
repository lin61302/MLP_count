#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Oct 13 2021

@author: diegoromero

This script obtains urls from the sitemap of the source and also from 
specific sections, and inserts them in the 'direct-urls' collection.
"""

import os
import getpass
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np 
import plotly.express as px
from tqdm import tqdm

from pymongo import MongoClient

import re
import bs4
from bs4 import BeautifulSoup
from newspaper import Article
from dateparser.search import search_dates
import dateparser
import requests
from urllib.parse import quote_plus

# for using the sleep() function
import time


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

urls = []

# URLs from Query:

endnumberx = 250
for i in range(1, endnumberx):
    if i == 1:
        url = 'https://neweasterneurope.eu/posts/'
    else:
        url = 'https://neweasterneurope.eu/posts/page/' + str(i) + '/'  
    
    print(url)

    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

# URLs from Sitemap:

for i in range(1, 5):
    url = 'https://neweasterneurope.eu/post-sitemap' + str(i) + '.xml'
    #print(url)
    print("Sitemap: ", url)

    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')


    for link in soup.findAll('loc'):
        urls.append(link.text)


# KEEP ONLY unique URLS:
list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Dropbox/ferloo_test.csv')  


## INSERTING URLS IN THE 'direct-urls' collection
# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

#df_urls = pd.read_csv("/Users/djrom/Dropbox/rsn1info_newurls.csv")

# Keeping unique URLs:
#list_urls = list(df_urls["url"])
#list_urls = list(set(list_urls))

# Upload URLs to the 'direct-urls' collection:
mycol = db['direct-urls']
for i in range(0, len(list_urls)):
  newurlx = list_urls[i]
  #newurl = { "url" : newurlx }
  #mycol.insert_one(newurl)
  #print(newurlx)
  l = [j for j in mycol.find(
    {
        'url': newurlx
    }
  )] 

  if l == []:
    newurl = { "url" : newurlx }
    mycol.insert_one(newurl)
    print(newurlx)
    

print("Done with neweasterneurope.eu")
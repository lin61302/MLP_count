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

## INSERTING URLS IN THE 'direct-urls' collection
# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}



## FROM SECTIONS: 

urls = []
# actualites I
for i in range(1, 100):
    if i == 1:
        url = 'https://lequotidien.sn/category/actualites/'
    else:
        url = 'https://lequotidien.sn/category/actualites/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls), 'de ',urls)

#time.sleep(61)

# Upload URLs to the 'direct-urls' collection:
mycol = db['direct-urls']
for i in range(0, len(list_urls)):
  newurlx = list_urls[i]
  #newurl = { "url" : newurlx }
  #mycol.insert_one(newurl)
  print(newurlx)
  l = [j for j in mycol.find(
    {
        'url': newurlx
    }
  )] 

  if l == []:
    newurl = { "url" : newurlx }
    mycol.insert_one(newurl)
    #print(newurlx)
    
print("Done with actualites I")

urls = []
# actualites I.5
for i in range(101, 376):
    if i == 1:
        url = 'https://lequotidien.sn/category/actualites/'
    else:
        url = 'https://lequotidien.sn/category/actualites/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#time.sleep(61)

# Upload URLs to the 'direct-urls' collection:
mycol = db['direct-urls']
for i in range(0, len(list_urls)):
  newurlx = list_urls[i]
  #newurl = { "url" : newurlx }
  #mycol.insert_one(newurl)
  print(newurlx)
  l = [j for j in mycol.find(
    {
        'url': newurlx
    }
  )] 

  if l == []:
    newurl = { "url" : newurlx }
    mycol.insert_one(newurl)
    #print(newurlx)
    
print("Done with actualites I.5")

urls = []
# actualites II
for i in range(376, 750):
    if i == 1:
        url = 'https://lequotidien.sn/category/actualites/'
    else:
        url = 'https://lequotidien.sn/category/actualites/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

#time.sleep(61)
list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#time.sleep(61)

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
    #print(newurlx)
    
print("Done with actualites II")


urls = []
# actualites III
for i in range(751, 1124):
    if i == 1:
        url = 'https://lequotidien.sn/category/actualites/'
    else:
        url = 'https://lequotidien.sn/category/actualites/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

#time.sleep(61)

list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#time.sleep(61)

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
    #print(newurlx)
    
print("Done with actualites III")


urls = []
# politique
for i in range(1, 270):
    if i == 1:
        url = 'https://lequotidien.sn/category/politique/'
    else:
        url = 'https://lequotidien.sn/category/politique/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 


list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#time.sleep(61)

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
    #print(newurlx)
    
print("Done with politique")


urls = []
# politique II
for i in range(271, 547):
    if i == 1:
        url = 'https://lequotidien.sn/category/politique/'
    else:
        url = 'https://lequotidien.sn/category/politique/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#time.sleep(61)

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
    #print(newurlx)
    
print("Done with politique II")

urls = []
# societe
for i in range(1, 141):
    if i == 1:
        url = 'https://lequotidien.sn/category/societe/'
    else:
        url = 'https://lequotidien.sn/category/societe/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#time.sleep(61)

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
    #print(newurlx)
    
print("Done with Societe")

urls = []
# economie
for i in range(1, 401):
    if i == 1:
        url = 'https://lequotidien.sn/category/economie/'
    else:
        url = 'https://lequotidien.sn/category/economie/page/' + str(i) + '/'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 

list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#time.sleep(61)

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
    #print(newurlx)
    
print("Done with economie")





# FROM KEY WORDS:
urls = []
# politique
for i in range(1, 979):
    if i == 1:
        url = 'https://lequotidien.sn/?s=politique'
    else:
        url = 'https://lequotidien.sn/page/' + str(i) + '/?s=politique'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 


# président
for i in range(1, 1734):
    if i == 1:
        url = 'https://lequotidien.sn/?s=pr%C3%A9sident'
    else:
        url = 'https://lequotidien.sn/page/' + str(i) + '/?s=pr%C3%A9sident'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 


# manifestation
for i in range(1, 199):
    if i == 1:
        url = 'https://lequotidien.sn/?s=manifestation'
    else:
        url = 'https://lequotidien.sn/page/' + str(i) + '/?s=manifestation'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 


# Assemblée Nationale
for i in range(1, 204):
    if i == 1:
        url = 'https://lequotidien.sn/?s=Assembl%C3%A9e+Nationale'
    else:
        url = 'https://lequotidien.sn/page/' + str(i) + '/?s=Assembl%C3%A9e+Nationale'
    #time.sleep(3)
    print(url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href')) 


# KEEP ONLY unique URLS:
list_urls = list(set(urls))

print('Number of Extracted URLs: ', len(list_urls))

#dftest = pd.DataFrame(list_urls)  
#dftest.to_csv('/Users/diegoromero/Dropbox/ferloo_test.csv')  



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
    

print("Done with lequotidien.sn")







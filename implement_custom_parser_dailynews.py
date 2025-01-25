#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Oct 25 14:50:01 2021

@author: diegoromero
"""

import sys
import re
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
# %pip install dateparser
import dateparser
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm



class UpdateDB:

    def __init__(self, col_to_scrape = None,
                    domain = None,
                    fix_date_publish = False,
                    fix_title = False,
                    fix_maintext = False,
                    start_year = None):

        '''
        Author: Tim McDade and Akanksha Bhattacharyya
        Date: 20 April 2021
        Modified by Diego Romero (Oct 25, 2021)
        '''

        self.db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p
        self.domain = domain
        #change according to your daterange
        #self.dates = pd.date_range(start=datetime(start_year,1,1), end=datetime.today(), freq='m')
        self.dates = pd.date_range(start=datetime(start_year,10,1), end=datetime(2021,12,1), freq='m')
        print(f'Initializing an instance of the UpdateDB class for {self.domain}.')

        ## Set all the flags.
        if col_to_scrape in ['articles-nodate', 'year_month']:
            self.col_to_scrape = col_to_scrape
        else:
            sys.exit('Error: pick a collection to scrape, either articles-nodate or year_month.')
            # break

        if fix_date_publish in [True, False]:
            self.fix_date_publish = fix_date_publish
        else:
            sys.exit('Error: pick whether fix_date_publish is True or False.')
            # break

        if fix_title in [True, False]:
            self.fix_title = fix_title
        else:
            sys.exit('Error: pick whether fix_title is True or False.')
            # break

        if fix_maintext in [True, False]:
            self.fix_maintext = fix_maintext
        else:
            sys.exit('Error: pick whether fix_maintext is True or False.')
            # break


    def dailynewscotz_story(self, soup):
        """
        Function to pull the information we want from dailynews.co.tz stories
        :param soup: BeautifulSoup object, ready to parse
        """
        hold_dict = {}
        
        # Get Title: 
        try:
            #article_title = soup.find("title").text
            contains_article = soup.find("meta", {"property":"og:title"})
            article_title = contains_article['content']
            hold_dict['title']  = article_title   
        except:
            article_title = None
            hold_dict['title']  = None
            
        # Get Main Text:
        try:
            maintext = soup.find("p", {"style":"text-align: justify;"}).text
            #maintext_contains = soup.findAll("p", {"style":"text-align: justify;"})
            #maintext = maintext_contains[0] + maintext_contains[1] + maintext_contains[2]
            hold_dict['maintext'] = maintext
        except: 
            maintext = None
            hold_dict['maintext']  = None

        # Get Date
        try:
            contains_date = soup.find("dd", {"class":"published"}).text
            commaindex = contains_date.index(",")
            contains_date = contains_date[commaindex+1:]
            article_date = dateparser.parse(contains_date, date_formats=['%d/%m/%Y'])
            hold_dict['date_publish'] = article_date
        except:
            try:
                contains_date = soup.find("div", {"class":"post-meta-date"}).text
                article_date = dateparser.parse(contains_date, date_formats=['%d/%m/%Y'])
                hold_dict['date_publish'] = article_date
            except:
                try:
                    contains_date = soup.find("dd", {"class":"publhed"}).text
                    commaindex = contains_date.index(",")
                    contains_date = contains_date[commaindex+1:]
                    article_date = dateparser.parse(contains_date, date_formats=['%d/%m/%Y'])
                    hold_dict['date_publish'] = article_date
                except:
                    article_date = None
                    hold_dict['date_publish'] = None  
        
        return hold_dict    


    def update_db(self, l, yr, mo):
        '''
        This is the code that actually does the updating.
        '''
        print('Beginning the actual update.')
        header = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36''(KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36')}
        lines = l
        data = []
        year = yr
        month = mo

        if year == None:
            col_name = f'articles-nodate'
        else:
            col_name = f'articles-{year}-{month}'

        for url,id in tqdm(lines):
            #print(url)
            # FIX URL
            #indexgraphic = url.find("/blog/")
            #newurl = "http://www.therwandan.com/" + url[(indexgraphic + 6):]  
            #print(newurl)

            d = self.db[col_name].find_one({'_id' : id})
            response = requests.get(url, headers=header).text
            soup = BeautifulSoup(response)

            # Fix maintext, if necessary.
            try:
                d['maintext'] = self.dailynewscotz_story(soup)['maintext'] #change
            except:
                d['maintext'] = None
            # print('The manually scraped maintext is ', d['maintext'])

            if d['maintext'] != None and self.fix_maintext == True:
                m = self.db[col_name].find_one({'_id': d['_id']})
                self.db[col_name].update_one(
                                        {'_id': d['_id']},
                                        {'$set': {'maintext': d['maintext']}}
                                        )

            # Fix title, if necessary.
            try:
                d['title'] = self.dailynewscotz_story(soup)['title'] #change
            except:
                d['title'] = None
            # print('The manually scraped title is ', d['title'])

            if d['title'] != None and self.fix_title == True:
                self.db[col_name].update_one(
                                        {'_id': d['_id']},
                                        {'$set': {'title': d['title']}}
                                        )

            # Fix date_publish, if necessary.
            try:
                d['date_publish'] = self.dailynewscotz_story(soup)['date_publish'] #change
            except:
                d['date_publish'] = None

            ## DELETE if not necessary - fix dates for some of the articles in the web.archive   
            if self.dailynewscotz_story(soup)['date_publish'] == None:
                if "web.archive.org" in url:
                    wordindex = url.index("/web/")
                    article_datew = url[wordindex+5:]
                    year_d = article_datew[0:4]
                    month_d = article_datew[4:6]
                    day_d = article_datew[6:8]
                    article_datex = datetime(int(year_d),int(month_d),int(day_d))
                    d['date_publish'] = article_datex
            #### 
            print('The manually scraped date_publish is ', d['date_publish'])

            if d['date_publish'] != None and self.fix_date_publish == True:
                new_year = d['date_publish'].year
                new_month = d['date_publish'].month
                d['year'] = new_year
                d['month'] = new_month
                new_col_name = f'articles-{new_year}-{new_month}'
                try:
                    self.db[col_name].delete_one({'url': d['url']})
                    self.db[new_col_name].insert_one(d)
                except DuplicateKeyError:
                    pass
        print('Update complete.')

    # def find_article_with_date(self, url, year, month):
    #     '''
    #     This updates the articles in the collection specific to a particular month.
    #     '''
    #     col_name = f'articles-{year}-{month}'
    #     d = [(i['url'], i['_id']) for i in self.db[col_name].find({'source_domain': self.domain})]
    #     return d


    def find_articles(self, url, year, month,
                                col_name,
                                title_filter_string = 'Página no encontrada',
                                maintext_filter_string = ''):
        '''
        This updates the articles in the articles-nodate collection.
        '''
        print('Finding the articles to be updated.')
        # col_name = f'articles-nodate'
        query = {
                    # {'$or': [{'title' : {'$regex' : re.compile(title_filter_string, re.IGNORECASE)}},
                    #          {'maintext' : {'$regex' : re.compile(maintext_filter_string, re.IGNORECASE)}}]},
                    # 'title' : {'$regex' : re.compile(title_filter_string, re.IGNORECASE)},
                    #'maintext' : {'$regex': 'bg_error_lines'},
                    ## Find all articles where the title/maintext is empty or
                    ## where the title/maintext contains some string
                    #'title' : {'$in': [None, re.compile(title_filter_string, re.IGNORECASE)]},
                    #'url': {'$not': re.compile('/blog/', re.IGNORECASE)},
                    #'url': {'$regex': '/blog/'},
                    #'download_via': {'$in': [None, '']},
                    #'download_via': {'$in': ['direct','wayback']},
                    'source_domain' : self.domain
                }
        to_update = [x for x in self.db[col_name].find(query)]
        # d = [(i['url'], i['_id']) for i in self.db[col_name].find({'source_domain': self.domain})]
        d = [(i['url'], i['_id']) for i in tqdm(to_update)]
        print('Articles found.')
        return d


    def main(self):
        '''
        This code runs the updates for whichever collection is supposed
        to be updated, according to the field self.col_to_scrape.
        '''
        #### if custom scraping 'articles-{year}-{month}'  collection
        if self.col_to_scrape == 'year_month':
            rgx = ''
            for date in tqdm(self.dates):
                year = date.year
                month = date.month
                col_name = f'articles-{year}-{month}'
                # l = find_articles(rgx, year, month) #change
                l = self.find_articles(rgx, year, month, col_name = col_name,
                                title_filter_string = '',
                                maintext_filter_string = '')
                self.update_db(l, year, month)
                print(col_name)

        #### if custom scraping 'articles-nodate'  collection
        elif self.col_to_scrape == 'articles-nodate':
            rgx = ''
            col_name = self.col_to_scrape
            l = self.find_articles(rgx, 2021, 3, col_name = col_name,
                                title_filter_string = '',
                                maintext_filter_string = '')
            self.update_db(l, None, None)

        print(f'All done with {self.domain}.')

if __name__ == "__main__":
    udb = UpdateDB(col_to_scrape = 'year_month',
                    domain = 'dailynews.co.tz',
                    fix_date_publish = True,
                    fix_title = True,
                    fix_maintext = False,
                    start_year = 2021)
    udb.main()
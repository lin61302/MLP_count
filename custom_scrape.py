import peacemachine.custom_parser as custom_parser
from pymongo import MongoClient
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from pymongo.errors import CursorNotFound, DuplicateKeyError
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from p_tqdm import p_umap
import os
from dotenv import load_dotenv

header = {
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36'	        
    '(KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36')	  
}



def insert(date, title, text, d, db):
    
    if title!=None:
        # year = d['date_publish'].year
        # month = d['date_publish'].month

        # col_name = f'articles-{year}-{month}'
        col_name = f'articles-nodate'
        try:
            db[col_name].update_one(
                {
                    '_id': d['_id']
                },
                {
                    '$set': {
                        'title': title
                    }
                }
            )
        except Exception as Err:
            print("Error: ", Err)
            
    if date !=None:
        # old_year = d['date_publish'].year
        # old_month = d['date_publish'].month
        new_year = date.year
        new_month = date.month
        d['date_publish'] = date
        # old_col_name = f'articles-{old_year}-{old_month}'
        old_col_name = f'articles-nodate'
        new_col_name = f'articles-{new_year}-{new_month}'
        try:
            db[new_col_name].delete_one(
            {
                'url': d['url']
            }
            )
            db[old_col_name].delete_one(
            {
                '_id': d['_id']
            })
            db[new_col_name].insert_one(d)
            print("Inserted in ", new_col_name)
            x = db[new_col_name].find_one(
            {
                '_id': d['_id']
            })
            print(x)
        except DuplicateKeyError:
            print("dup")
            pass

    
    if text!=None:
        
        year = date.year
        month = date.month

        col_name = f'articles-{year}-{month}'
        try:
            db[col_name].update_one(
                {
                    '_id': d['_id']
                },
                {
                    '$set': {
                        'maintext': text
                    }
                }
            )
            # print(text)
        except Exception as Err:
            print("Error: ", Err)
    
    
        
        
    return


def customparse(docs, db): 
    # print(docs[0:10])
    for d in docs:
        if 'feed/' in d['url']:
            d['url'] = d['url'][:-5]
        print(d['url'])
        d['url'] = "/".join(d['url'].split("/")[:-2])
        response = requests.get(d['url'], headers=header).text
        soup = BeautifulSoup(response)
        function_name = db.sources.find_one({'source_domain' : d['source_domain']})['custom_parser']
        try:
            if function_name != '':
                domain_parser = getattr(custom_parser, function_name)
                date = domain_parser(soup)['date_publish']
                # text = domain_parser(soup)['maintext']
                title = domain_parser(soup)['title']
                
                insert(date, title, None, d, db)
                # insert(None, None, text, d, db)
                # insert(date, None, None, d, db)
        except Exception as err:
            print(err)

            print("ERROR!")



def main():
    load_dotenv()
    uri = os.getenv('DATABASE_URL')
    db = MongoClient(uri).ml4p
    
    batch_size = 128
    
    sd = db.sources.distinct('source_domain', filter={'include' : True, 'source_domain' : 'telegraf.al'})

    # dates = pd.date_range('2016-7-1', datetime.now()+relativedelta(months=1) , freq='M')
    
    # for date in dates:
    #     try:
    #         cursor = db[f'articles-{date.year}-{date.month}'].find({
    #             'source_domain' : {'$in' : sd},
    #             'title' : {'$regex' : '- Kosova Sot'},
    #         }).batch_size(batch_size)

    #         list_docs = []
    #         for _doc in tqdm(cursor):
    #             list_docs.append(_doc)
    #             if len(list_docs) >= batch_size:
    #                 print('Extracting urls')
    #                 try:
    #                     customparse(list_docs, db)
    #                 except ValueError:
    #                     print('ValueError')
    #                 except AttributeError:
    #                     print('AttributeError')
    #                 except Exception as err:
    #                     print(err)
    #                 list_docs = []
    #         customparse(list_docs, db)
    #         list_urls = []
    #     except CursorNotFound:
    #         pass

    try:
        cursor = db[f'articles-nodate'].find({
            'source_domain' : {'$in' : sd},
            'title' : {'$ne' : None},
            'download_via' : 'direct',
        }).batch_size(batch_size)

        list_docs = []
        for _doc in tqdm(cursor):
            list_docs.append(_doc)
            if len(list_docs) >= batch_size:
                print('Extracting urls')
                try:
                    customparse(list_docs, db)
                except ValueError:
                    print('ValueError')
                except AttributeError:
                    print('AttributeError')
                except Exception as err:
                    print(err)
                list_docs = []
        customparse(list_docs, db)
        list_urls = []
    except CursorNotFound:
        pass


if __name__ == '__main__':
    main()



'''
2022-3-5, Zung-Ru
'''

import orjson as json
import os
from pathlib import Path
import re
import pandas as pd
from tqdm import tqdm
from p_tqdm import p_umap
import time
from dotenv import load_dotenv
from pymongo import MongoClient
import multiprocessing
import dateparser
from os import listdir
from datetime import datetime,timedelta


def keywords(folder_to_keywords):

    db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p
    files = ["kw_CR_en", "kw_HRA_en", "kw_HS_en", "kw_IDP_en"]
    dic = {}
    for file in files:
        dic[f'{file}_path'] = f'{folder_to_keywords}{file}.xlsx'
        dic[f'{file}_file'] = pd.read_excel(dic[f'{file}_path'])
        dic[f'{file}_keyword'] = dic[f'{file}_file']['CompanyName'].str.strip()
        dic[f'{file}_ind'] = dic[f'{file}_file']['alphabet_connect']


    for file in files:
        final_kw = []
        for i, kw in enumerate(dic[f'{file}_keyword']):
            try:
                if bool(int(dic[f'{file}_ind'][i])):
                    final_kw.append(dic[f'{file}_keyword'][i][2:-2].rstrip().lstrip())

                else:
                    final_kw.append("(?<![a-z])" + dic[f'{file}_keyword'][i][2:-2].rstrip().lstrip() + "(?![a-z])")
            
            except:
                pass

        dic[f'{file}_final_string'] = '|'.join(final_kw)

    
    cur = db['ukr_cities'].find()
    ukr_cities = list(set(cur[0].keys()))
    cities_string ='|'.join(ukr_cities)




    CR_filter = re.compile(dic['kw_CR_en_final_string'],flags=re.IGNORECASE)
    HRA_filter = re.compile(dic['kw_HRA_en_final_string'],flags=re.IGNORECASE)
    HS_filter = re.compile(dic['kw_HS_en_final_string'],flags=re.IGNORECASE)
    IDP_filter = re.compile(dic['kw_IDP_en_final_string'],flags=re.IGNORECASE)
    cities_filter = re.compile(cities_string,flags=re.IGNORECASE)

    return CR_filter,  HRA_filter, HS_filter, IDP_filter, cities_filter





def run_upload(language, keyword, folder_path, files_you_want_to_upload, files_done_upload, path_to_reference,CR_filter,  HRA_filter, HS_filter, IDP_filter, cities_filter, sleep_time):
    process = upload(language, keyword, folder_path, files_you_want_to_upload, files_done_upload, path_to_reference,CR_filter,  HRA_filter, HS_filter, IDP_filter, cities_filter, sleep_time)
    process.run()
    


class upload:

    def __init__(self, language, keyword, folder_path, files_you_want_to_upload, files_done_upload, path_to_reference,CR_filter,  HRA_filter, HS_filter, IDP_filter, cities_filter, sleep_time ):
        self.db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p
        #self.client = MongoClient('mongodb://ml4pdevlab:h8Nz%oJ^GtgcLE@sample-cluster.node.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false') 

        # self.client = MongoClient('mongodb://ml4pdevlab:h8Nz%oJ^GtgcLE@docdb-2022-03-05-23-14-29.cluster-crmvlpxjevij.us-east-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
        self.language = language
        self.keyword = keyword
        self.folder_path = folder_path
        self.files_you_want_to_upload = files_you_want_to_upload
        self.files_done_upload = files_done_upload
        self.reference = pd.read_csv(path_to_reference)
        self.CR_filter = CR_filter
        self.HRA_filter = HRA_filter
        self.HS_filter = HS_filter
        self.IDP_filter = IDP_filter 
        self.cities_filter = cities_filter
        self.sleep_time = sleep_time

        cur = self.db['ukr_cities'].find()
        dic_ukr_cities = [dic for dic in cur][0]

        self.dic_ukr_cities = dic_ukr_cities

        cur2 = self.db['eng_cities'].find()
        dic_eng_cities = [dic for dic in cur2][0]

        self.dic_eng_cities = dic_eng_cities

        self.txt_path = self.folder_path + 'files_uploaded.txt'




        

    

    
    def get_files(self):

        folder_files = [f for f in listdir(self.folder_path)]
        no_list = []
        
        
        files_uploaded = []
        with open(self.txt_path, 'r') as fileobj:
            for row in fileobj:
                files_uploaded.append(row.rstrip('\n'))
        

        for file in folder_files:
            if ('.json' not in file) or (self.keyword not in file) or ('.gz' in file) or ('.tar' in file):
                no_list.append(file)
                continue

            for str in self.files_done_upload:
                if str in file:
                    no_list.append(file)
                    break
        
        if self.files_you_want_to_upload == []:
            files = [self.folder_path + f for f in folder_files if f not in no_list]
            files = [f2 for f2 in files if f2 not in files_uploaded]
            files = sorted(files)

            return files


        else:
            files = []
            
            for str in self.files_you_want_to_upload:
                for file in folder_files:
                    if (str in file) and ('.json' in file) and (self.keyword in file):
                        files.append(file)
                        continue
            files = [self.folder_path + f for f in files if f not in no_list]
            files = [f2 for f2 in files if f2 not in files_uploaded]
            files = sorted(files)
            

            return files





    def change_column_names(self, doc):

        tweet_rename_dic ={'id_str':'IDtweet', 'text':'text', 'created_at':'created_hub',  'lang':'language','source':'source_hub', 
                            'in_reply_to_status_id_str':'IDinReply',  'in_reply_to_screen_name':'name_inReply_hub', 'retweeted':'retweeted'}

        user_rename_dic = {'screen_name':'name_hub', 'verified':'verified_hub', 'friends_count':'friends_hub', 'followers_count':'followers_hub', 
                            'location':'location_hub', 'id_str':'idstr_hub', 'created_at':'account_created'}

        retweet_rename_dic = {'created_at':'created_auth', 'favorite_count':'favorites_auth', 'id_str':'IDtweet_auth', 
                            'in_reply_to_screen_name':'name_inReply_auth'}
        
        retweet_user_rename_dic = {'screen_name':'name_auth', 'verified':'verified_auth', 'friends_count':'friends_auth', 
                            'followers_count':'followers_auth', 'location':'location_auth', 'id_str':'idstr_auth' }

        entity_rename_dic = {'url':'web'}

        
        doc2={}

        doc2['keyword'] = self.keyword
        

        for key, value in tweet_rename_dic.items():
            try:
                doc2[value] = doc[key]
            except:
                None
            
        for key, value in user_rename_dic.items():
            try:
                doc2[value] = doc['user'][key]
            except:
                None
            
        for key, value in retweet_rename_dic.items():
            try:
                doc2[value] = doc['retweeted_status'][key]
            except:
                doc2[value] = None

        for key, value in retweet_user_rename_dic.items():
            try:
                doc2[value] = doc['retweeted_status']['user'][key]
            except:
                doc2[value] = None

        for key, value in entity_rename_dic.items():
            try:
                doc2[value] = doc['entities']['urls'][0][key]
            except:
                doc2[value] = None



        return doc2
    



    def get_date(self, doc):

        try:
            date_hub = doc['created_hub']
            date_hub = date_hub [-4:]+' '+ date_hub [4:-10]
            date_hub_final = dateparser.parse(date_hub).replace(tzinfo = None)
        except:
            date_hub_final = None


        try:
            date_auth = doc['created_auth']
            date_auth = date_auth [-4:]+' '+ date_auth [4:-10]
            date_auth_final = dateparser.parse(date_auth).replace(tzinfo = None)
        except:
            date_auth_final = None


        try:
            date_acc = doc['account_created']
            date_acc = date_acc [-4:]+' '+ date_acc [4:-10]
            date_acc_final = dateparser.parse(date_acc).replace(tzinfo = None)
        except:
            date_acc_final = None

        doc['created_hub'] = date_hub_final
        doc['created_auth'] = date_auth_final 
        doc['account_created'] = date_acc_final

        try:
            doc['created_hour'] = date_hub_final.hour
            doc['created_minute'] = date_hub_final.minute
                
        except:
            doc['created_hour'] = None
            doc['created_minute'] =  None


        return doc




    def map_reference(self, doc):

        names = set(self.reference['name'])

        if doc['name_hub'] in names:

            doc['membership_hub'] = float(self.reference.loc[self.reference['name'] == doc['name_hub'], 'membership'])
            doc['l1_hub'] = float(self.reference.loc[self.reference['name'] == doc['name_hub'], 'l1'])
            doc['l2_hub'] = float(self.reference.loc[self.reference['name'] == doc['name_hub'], 'l2'])


        if doc['name_auth'] in names:

            doc['membership_auth'] = float(self.reference.loc[self.reference['name'] == doc['name_auth'], 'membership'])
            doc['l1_auth'] = float(self.reference.loc[self.reference['name'] == doc['name_auth'], 'l1'])
            doc['l2_auth'] = float(self.reference.loc[self.reference['name'] == doc['name_auth'], 'l2'])

        
        return doc

    def check_keyword(self, doc, type):   
        try:
            if type == 'CR':
                if bool(CR_filter.search(doc)):
                    return True
                else:
                    return False
            elif type == 'HRA':
                if bool(HRA_filter.search(doc)):
                    return True
                else:
                    return False
            elif type == 'HS':
                if bool(HS_filter.search(doc)):
                    return True
                else:
                    return False
            elif type == 'IDP':
                if bool(IDP_filter.search(doc)):
                    return True
                else:
                    return False
        
            else:
                print('correct the type')

        except:
            return False


    def text_locations(self, doc):
        locations = []
        locations_info = []
        
        ukr_cities_list = list(self.dic_ukr_cities.keys())[1:] 
        eng_cities_list = list(self.dic_eng_cities.keys())[1:]

        for city in ukr_cities_list:
            if city in doc['text']:    
                locations.append(city)
                dic = self.dic_ukr_cities[city]
                dic['city'] = city
                locations_info.append(dic)

        for city in eng_cities_list:
            if city in doc['text']:    
                locations.append(city)
                dic = self.dic_eng_cities[city]
                dic['city'] = city
                locations_info.append(dic)

        doc['ukr_locations'] = locations
        doc['ukr_locations_info'] = locations_info

        return doc
                
        
        
            




    def upload_data(self, doc, num, file):

        date = doc['created_hub'] 

        if self.language == 'ru':
            colname = f'tweets-{date.year}-{date.month}-{date.day}-russian'
            l = [j for j in self.db[colname].find({'IDtweet': doc['IDtweet']} )]

        elif self.language == 'uk':
            colname = f'tweets-{date.year}-{date.month}-{date.day}-ukrainian'
            l = [j for j in self.db[colname].find({'IDtweet': doc['IDtweet']} )]

        else:
            colname = f'tweets-{date.year}-{date.month}-{date.day}'
            l=[]
            



        # l = [j for j in self.db[colname].find({'IDtweet': doc['IDtweet']} )]

        try:
            if l == []:
                self.db[colname].insert_one(doc)
                print(f'{num}---{file}: uploading to {colname}')

            else:

                print(f'{num}---{file}: exists, pass! ')

        

        except:
            #db[colname].update_one({'IDtweet': doc['IDtweet']},{'$set': {"keyword": "ukraine"}})
            #print(f'{num}---{file}: updating ', colname)
            #self.db[colname].update_one(doc)
            print(f'{num}---{file}: exists, pass! ')

    # def upload_data(self, docs, num, file):

    #     for i,doc in enumerate(docs):
    #         date = doc['created_hub'] 
    #         colname = f'tweets-{date.year}-{date.month}-{date.day}'


            

    #         try:
    #             self.db[colname].insert_one(doc)
    #             print(f'{num-98+i}---{file}: uploading to {colname}')
            

    #         except:
    #             #db[colname].update_one({'IDtweet': doc['IDtweet']},{'$set': {"keyword": "ukraine"}})
    #             #print(f'{num}---{file}: updating ', colname)
    #             #self.db[colname].update_one(doc)
    #             print(f'{num-98+i}---{file}: exists, pass! ')
            


    
    def run(self):

        while True:

            files = self.get_files()

            if len(files)!=0:

                for file in files:

                    print(f'Start: {file} ')
                
                    
                    for i, line in enumerate(open(file, 'r')):
                        

                        doc = json.loads(line)
                        # print(doc['text'])
                        doc  = self.change_column_names(doc)
                        doc = self.get_date(doc)
                        doc2 = self.map_reference(doc)
                        doc_final = self.text_locations(doc2)

                        events = []
                        if self.check_keyword(doc['text'], 'CR'):
                            events.append('CR')
                        if self.check_keyword(doc['text'], 'HRA'):
                            events.append('HRA')
                        if self.check_keyword(doc['text'], 'HS'):
                            events.append('HS')
                        if self.check_keyword(doc['text'], 'IDP'):
                            events.append('IDP')
                        doc_final["event_type"] = events

                        if 'RT' in doc_final['text'][:3]:
                            doc_final['retweet?'] = 'Yes'
                        else:
                            doc_final['retweet?'] = 'No'

                        

                    
                        

                        

                        proc = multiprocessing.Process(self.upload_data(doc_final, i, file))
                        proc.start()


                    print(f'Done: {file}')
                    write_file = [file]
                    write_file = [ff+'\n' for ff in write_file]

                    print(f"Write files --- {file} \n\n to {self.txt_path}")

                    with open(self.txt_path, 'a+') as f:
                        f.writelines(write_file)




                
                    

                

            
                

                start_time = datetime.now()
                finish_time = start_time + timedelta(seconds=self.sleep_time)
                print(f"Start sleeping: {start_time.hour}:{start_time.minute}/ Finish at {finish_time.hour}:{finish_time.minute}")
                time.sleep(self.sleep_time)

            
            else:
                print("No uppload needed!!!!")
                start_time = datetime.now()
                finish_time = start_time + timedelta(seconds=self.sleep_time)
                print(f"Start sleeping: {start_time.hour}:{start_time.minute}/ Finish at {finish_time.hour}:{finish_time.minute}")
                time.sleep(self.sleep_time)





if __name__ == '__main__':
    '''
    (arg):
    
    keyword --- keyword,
    folder_path ---  path to your files, used to iterate the whole folder for documents to be uploaded
    files_done_upload --- files you DON'T wanna upload, can be a few words, date, any string
    files_you_want_to_upload --- files you DON'T wanna upload, can be a few words, date, any string
    path_to_reference --- file path to key account reference

    '''
    

########################### !!!Make sure you've filled this section before running (language, keyword, folder_path, path_to_reference, folder_to_keywords)!!!################################################
   
    language = 'uk'        # uk, ru, en
    keyword = 'російський'   # keyword used to identify files
    folder_path = '/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/'  # folder path to your files
    files_you_want_to_upload = ['російський03']    # a piece of string, file name: you **WANT** to upload  

    files_done_upload = [] 

    path_to_reference = '/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/reference.data3.csv'

    #Done 0301, 0304,0305
    folder_to_keywords = '/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/'   ##remember add / at the end

    sleep_time = 1800

##########################################################################################################################################################################################################################
    
    CR_filter,  HRA_filter, HS_filter, IDP_filter, cities_filter = keywords(folder_to_keywords)
    run_upload(language = language, keyword = keyword, folder_path = folder_path, files_you_want_to_upload = files_you_want_to_upload, files_done_upload = files_done_upload, path_to_reference = path_to_reference, CR_filter= CR_filter,  HRA_filter=HRA_filter, HS_filter=HS_filter, IDP_filter=IDP_filter, cities_filter=cities_filter, sleep_time = sleep_time)

## Uploaded so far:
# March 5, 2022: ['російський03032022a.json', 'російський03032022b.json', 'російський03032022c.json', 'російський03032022d.json', 'російський03032022e.json', 'російський03032022f.json', 'російський03032022g.json'] 
# March 5, 2022: ['російський03042022a.json', 'російський03042022b.json', 'російський03042022c.json']

# March 6, 2022: ['російський03052022d.json', 'російський03052022e.json', 'російський03052022f.json', 'російський03052022g.json', 'російський03052022h.json', 'російський03052022i.json'] 
# March 8, 2022: ['російський03032022a.json', 'російський03032022b.json', 'російський03032022c.json', 'російський03032022d.json', 'російський03032022e.json', 'російський03032022f.json', 'російський03032022g.json', 'російський03062022j.json', 'російський03072022a.json', 'російський03082022b.json', 'російський03082022c.json', 'російський03082022d.json', 'російський03082022e.json', 'російський03082022f.json']
# 
# March 10, 2022: 'російський0306','російський0307','російський0308','російський0309','російський0310' up to російський03102022q
# March 11, 2022: up to російський03112022u.json
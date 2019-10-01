"""Saayan"""

import threading
from multiprocessing import Process
import pymongo
import sys
import os
import csv
from urllib2 import URLError,HTTPError
import socket
import datetime
import _mysql_exceptions
import unidecode
import pinyin
import urllib2
import json
import httplib
import unidecode
sys.setrecursionlimit(1500)

class rovi_ingestion:
    #TODO: INITIALIZATION
    def __init__(self):
        self.rovi_id=''
        self.rovi_sm_id=''
        self.rovi_show_type=''
        self.iso_character_language=''
        self.release_year=0
        self.record_language=''
        self.episode_title=''
        self.title=''

    #TODO: one time call param 
    def constant_param(self):
        self.rovi_mo_list=[]
        self.rovi_sm_list=[]
        self.rovi_se_list=[]
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'             

    def get_env_url(self):
        self.source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi"

    #TODO: Mongoconnection set up
    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017")
        self.mydb=self.connection["qadb"]
        self.mytable=self.mydb["program_general"]    

    #TODO : To check Rovi_ids ingestion
    def ingestion_checking(self,thread_name):
        print("Checking ingestion of rovi series and Movies to Projectx ..........................",thread_name)
        #import pdb;pdb.set_trace()
        retry_count=0
        try:
            print ("\n")
            print (self.rovi_id,self.rovi_show_type,thread_name)
            if self.rovi_show_type=='MO' or self.rovi_show_type=='SE' or self.rovi_show_type=='SM':
                rovi_resp=urllib2.urlopen(urllib2.Request(self.source_mapping_api%eval(self.rovi_id),None,{'Authorization':self.token}))
                data_rovi=rovi_resp.read()
                data_rovi_resp=json.loads(data_rovi)
                if data_rovi_resp!=[]:
                    self.writer.writerow([self.rovi_sm_id,self.rovi_id,self.rovi_show_type,self.iso_character_language,self.release_year,
                    	                               self.record_language,self.episode_title,self.title,'','','True','','']) 
                else:
                    self.writer.writerow([self.rovi_sm_id,self.rovi_id,self.rovi_show_type,self.iso_character_language,self.release_year,
                    	                           self.record_language,self.episode_title,self.title,'','','False','',''])
        except (IOError,Exception,httplib.BadStatusLine,HTTPError,URLError,RuntimeError,ValueError) as e:
            retry_count+=1
            print ("exception caught ................................",type(e),thread_name,self.rovi_id,self.rovi_sm_id,self.rovi_show_type)
            print ("\n") 
            print ("Retrying.............")
            print ("\n")  
            if retry_count<=5:  
                self.ingestion_checking(thread_name)
            else:
                retry_count=0           

    #TODO: to open file to write
    def create_csv(self,result_sheet):
        if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file

    def main(self,start_id,thread_name,end_id,page_id):
        result_sheet='/result/Ingestion_checked_in_Px%d.csv'%page_id
        fieldnames = ["rovi_sm_id","rovi_id","rovi_show_type","iso_character_language","release_year","record_language","episode_title","title"
                                         ,"Duplicate_present","Series_duplicate_present","Ingested","Not_ingested","Data_present_in_dump"]
        output_file=self.create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(fieldnames)
            #import pdb;pdb.set_trace()
            for aa in range(start_id,end_id,10):
                try:
                    #import pdb;pdb.set_trace()
                    rovi_query=self.mytable.aggregate([{"$match":{"$and":[{"show type":{"$in":["SE","MO","SM"]}}]}},{"$project":{"program id":1,"series id":1,"_id":0,"show type":1,"iso 3 character language":1,"release year":1,"record language":1,"episode title":1,"long title":1}},{"$skip":aa},{"$limit":10}]) 
                    print({"start": start_id,"end":end_id,"thread_name":thread_name})
                    print ("\n")
                    #import pdb;pdb.set_trace()
                    for data in rovi_query:
                        #import pdb;pdb.set_trace()
                        self.__init__()
                        self.rovi_id=data.get("program id").encode('utf-8')
                        self.rovi_sm_id=data.get("series id").encode('utf-8')
                        self.iso_character_language=data.get("iso 3 character language").encode('utf-8')
                        self.release_year=data.get("release year").encode('utf-8')
                        self.record_language=data.get("record language").encode('utf-8')
                        self.episode_title=unidecode.unidecode(pinyin.get(unicode(data.get("episode title"))))
                        self.title=unidecode.unidecode(pinyin.get(unicode(data.get("long title"))))
                        self.rovi_show_type=data.get("show type").encode('utf-8').encode('utf-8')
                        if self.rovi_id is not None or self.rovi_id!="":
                            if self.rovi_show_type=='MO' and self.rovi_id not in self.rovi_mo_list:
                                    self.rovi_mo_list.append(self.rovi_id)
                            elif self.rovi_show_type=='SM' and self.rovi_id not in self.rovi_sm_list:
                                    self.rovi_sm_list.append(self.rovi_id)
                            elif self.rovi_show_type=='SE' and self.rovi_id not in self.rovi_se_list:
                                    self.rovi_se_list.append(self.rovi_id)
                        #self.getting_source_ids(data,thread_name)
                        print ({"Total":len(self.rovi_mo_list)+len(self.rovi_se_list)+len(self.rovi_sm_list),"Thread_name":thread_name})
                        print ("\n")
                        print datetime.datetime.now()
                        self.ingestion_checking(thread_name)
                except (Exception,socket.error,pymongo.errors.CursorNotFound) as e:
                    pass        
        output_file.close()

    # TODO : to run threads in Thread pool
    def thread_pool(self):   

        t1=Process(target=self.main,args=(0,"thread - 1",50000,1))
        t1.start()
        t2=Process(target=self.main,args=(50000,"thread - 2",100000,2))
        t2.start()                          
        t3=Process(target=self.main,args=(100000,"thread - 3",150000,3))
        t3.start()
        t4=Process(target=self.main,args=(150000,"thread - 4",200000,4))
        t4.start()
        t5=Process(target=self.main,args=(200000,"thread - 5",250000,5))
        t5.start()
        t6=Process(target=self.main,args=(250000,"thread - 6",300000,6))
        t6.start()
        t7=Process(target=self.main,args=(300000,"thread - 7",350000,7))
        t7.start()
        t8=Process(target=self.main,args=(350000,"thread - 8",400000,8))
        t8.start()
        t9=Process(target=self.main,args=(400000,"thread - 9",450000,9))
        t9.start()
        t10=Process(target=self.main,args=(450000,"thread - 10",500000,10))
        t10.start()                          
        t11=Process(target=self.main,args=(500000,"thread - 11",550000,11))
        t11.start()
        t12=Process(target=self.main,args=(550000,"thread - 12",600000,12))
        t12.start()
        t13=Process(target=self.main,args=(600000,"thread - 13",650000,13))
        t13.start()
        t14=Process(target=self.main,args=(650000,"thread - 14",700000,14))
        t14.start()
        t15=Process(target=self.main,args=(700000,"thread - 15",750000,15))
        t15.start()
        t16=Process(target=self.main,args=(750000,"thread - 16",800000,16))
        t16.start()
        t17=Process(target=self.main,args=(800000,"thread - 17",850000,17))
        t17.start()
        t18=Process(target=self.main,args=(850000,"thread - 18",900000,18))
        t18.start()

        # TODO : To close connection
        self.connection.close()

#starting and calling functions 
object_=rovi_ingestion()
object_.__init__()
object_.constant_param()
object_.get_env_url()
object_.mongo_connection()
object_.thread_pool()    
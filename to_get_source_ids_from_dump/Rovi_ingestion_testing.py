"""Saayan"""

import threading
from multiprocessing import Process
import sys
import os
import csv
import pymongo
import socket
import datetime
import urllib2
import json
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
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
        self.writer=''

    #TODO: one time call param 
    def constant_param(self):
        self.rovi_mo_list=[]
        self.rovi_sm_list=[]
        self.rovi_se_list=[]
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'             

    #TODO: Mongoconnection set up
    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017")
        self.mydb=self.connection["qadb"]
        self.mytable=self.mydb["program_general"]               

    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        result_sheet='/output_rovi/Ingestion_checked_in_Px_rovi_%s_%s.csv'%(thread_name,datetime.date.today())
        fieldnames = ["rovi_sm_id","rovi_id","rovi_show_type"]
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
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
                        self.rovi_id=data.get("program id").encode('utf-8')
                        self.rovi_sm_id=data.get("series id").encode('utf-8')
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
                        self.writer.writerow([self.rovi_sm_id,self.rovi_id,self.rovi_show_type])
                except (Exception,socket.error,pymongo.errors.CursorNotFound) as e:
                    pass        
        output_file.close()

    # TODO : to run threads in Thread pool
    def thread_pool(self):   

        t1=Process(target=self.main,args=(0,"thread-1",50000))
        t1.start()
        t2=Process(target=self.main,args=(50000,"thread-2",100000))
        t2.start()                          
        t3=Process(target=self.main,args=(100000,"thread-3",150000))
        t3.start()
        t4=Process(target=self.main,args=(150000,"thread-4",200000))
        t4.start()
        t5=Process(target=self.main,args=(200000,"thread-5",250000))
        t5.start()
        t6=Process(target=self.main,args=(250000,"thread-6",300000))
        t6.start()
        t7=Process(target=self.main,args=(300000,"thread-7",350000))
        t7.start()
        t8=Process(target=self.main,args=(350000,"thread-8",400000))
        t8.start()
        t9=Process(target=self.main,args=(400000,"thread-9",450000))
        t9.start()
        t10=Process(target=self.main,args=(450000,"thread-10",500000))
        t10.start()                          
        t11=Process(target=self.main,args=(500000,"thread-11",550000))
        t11.start()
        t12=Process(target=self.main,args=(550000,"thread-12",600000))
        t12.start()
        t13=Process(target=self.main,args=(600000,"thread-13",650000))
        t13.start()
        t14=Process(target=self.main,args=(650000,"thread-14",700000))
        t14.start()
        t15=Process(target=self.main,args=(700000,"thread-15",750000))
        t15.start()
        t16=Process(target=self.main,args=(750000,"thread-16",800000))
        t16.start()
        t17=Process(target=self.main,args=(800000,"thread-17",850000))
        t17.start()
        t18=Process(target=self.main,args=(850000,"thread-18",900000))
        t18.start()

        # TODO : To close connection
        self.connection.close()

#starting and calling functions 
object_=rovi_ingestion()
object_.constant_param()
object_.mongo_connection()
object_.thread_pool()    
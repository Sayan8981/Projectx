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

class vudu_ids:
    #TODO: INITIALIZATION
    def __init__(self):
        self.source='Vudu'
        self.total=0

    #TODO: Mongoconnection set up
    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
        self.mydb=self.connection["qadb"]
        self.sourceidtable=self.mydb["vududump"]  

    #TODO: to get source_details 
    def getting_source_details(self,data): 
        #import pdb;pdb.set_trace()
        series_id=0
        _id=data.get("launch_id").encode()
        show_type=data.get("show_type").encode()
        if show_type=='SE':
            series_id=data.get("series_id")
        print ({"vudu_id":_id,"series_id":series_id,"show_type":show_type,"total":self.total})
        purchase_type=data.get("purchase_types")
        if purchase_type!=[]:
            purchase_type='True'
        else:
            purchase_type='Null'
        return {"series_id":series_id,"_id":_id,"show_type":show_type,"purchase_type":purchase_type}                     

    def main(self,start_id,thread_name,end_id):
        ##import pdb;pdb.set_trace()
        print({"start":start_id,"end":end_id}) 
        fieldnames = ["%s_series_id"%self.source,"%s_id"%self.source,"show_type","purchase_type"]   
        result_sheet='/output_vudu/%s_id_%s_%s.csv'%(self.source,thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            writer.writerow(fieldnames)
            #import pdb;pdb.set_trace()
            for aa in range(start_id,end_id,10):
                try:
                    id_query=self.sourceidtable.aggregate([{"$skip":aa},{"$limit":10},{"$match":{"$and":[{"show_type":{"$in":["MO","SE","SM"]}}]}},{"$project":{"launch_id":1,"_id":0,"show_type":1,"title":1,"release_year":1,"series_id":1,"series_title":1,"episode_number":1,"language":1,"purchase_types":1}}])
                    for data in id_query:
                        self.total+=1
                        print({"thread_name":thread_name})
                        details=self.getting_source_details(data)
                        writer.writerow([details["series_id"],details["_id"],details["show_type"],details["purchase_type"],data.get("language")])
                except (Exception,pymongo.errors.CursorNotFound) as e:
                    pass        
        output_file.close()

    # TODO : to run threads in Thread pool
    def thread_pool(self):   

        t1=Process(target=self.main,args=(0,"thread-1",20000))
        t1.start()
        t2=Process(target=self.main,args=(20000,"thread-2",40000))
        t2.start()
        t3=Process(target=self.main,args=(40000,"thread-3",60000))
        t3.start()
        t4=Process(target=self.main,args=(60000,"thread-4",80000,))
        t4.start()
        t5=Process(target=self.main,args=(80000,"thread-5",100000))
        t5.start()
        t6=Process(target=self.main,args=(100000,"thread-6",120000))
        t6.start()
        t7=Process(target=self.main,args=(120000,"thread-7",140000))
        t7.start()
        t8=Process(target=self.main,args=(140000,"thread-8",160000))
        t8.start()
        t9=Process(target=self.main,args=(160000,"thread-9",180000))
        t9.start()
        t10=Process(target=self.main,args=(180000,"thread-10",200000))
        t10.start()
        t11=Process(target=self.main,args=(200000,"thread-11",220000))
        t11.start()
        t12=Process(target=self.main,args=(220000,"thread-12",240000))
        t12.start()

        # TODO : To close connection
        self.connection.close()

#starting and calling functions 
object_=vudu_ids()
object_.mongo_connection()
object_.thread_pool()    
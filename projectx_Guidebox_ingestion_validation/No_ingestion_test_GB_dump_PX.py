"""Saayan"""

import threading
import pymongo
import sys
import os
import csv
import re
from urllib2 import HTTPError,URLError
import socket
import datetime
import urllib2
import json
import httplib
import unidecode
sys.setrecursionlimit(1500)

class GB_ingestion:

    #TODO: INITIALIZATION
    def __init__(self):
        self.writer=''
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.gb_mo_list=[]
        self.gb_sm_list=[]
        self.gb_se_list=[]

    def get_env_url(self):         
        self.source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=%s"
        self.source_duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=%s"
        self.check_presence_in_DB="http://34.231.212.186:81/projectx/guidebox?sourceId=%d&showType=%s"
        self.px_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'
    
    # set up connection of DB
    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
        self.mydb=self.connection["ozone"]
        self.mytable=self.mydb["guidebox_program_details"]
    
    #TODO: fetching response for the given API
    def fetch_response_for_id(self,api,token):
        resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':token}))
        data = resp.read()
        data_resp = json.loads(data)
        return data_resp

    #TODO: getting duplicate source_ids for duplicate px_ids
    def getting_duplicate_source_id(self,px_id,show_type):
        retry_count=0
        source_list=[]
        for _id in px_id:
            resp=urllib2.urlopen(urllib2.Request(self.px_mapping_api%_id,None,{'Authorization':self.token}))
            data_resp_mapping=json.loads(resp.read())
            for resp in data_resp_mapping:
                if resp.get("data_source")=='GuideBox' and resp.get("sub_type")==show_type and resp.get("type")=='Program':
                    source_list.append(resp.get("sourceId"))
        return source_list

    #TODO: to check GB_id ingestion
    def ingestion_checking(self,gb_id,gb_sm_id,gb_show_type,thread_name):
        #import pdb;pdb.set_trace()
        retry_count=0
        duplicate_px_id=[]
        try:
            print ("\n")
            print (gb_id,gb_show_type,thread_name)
            guidebox_mapping_api=self.source_mapping_api%(gb_id,gb_show_type.encode())
            data_GB_resp=self.fetch_response_for_id(guidebox_mapping_api,self.token)
            if data_GB_resp!=[]:  
                self.writer.writerow([gb_sm_id,gb_id,gb_show_type,'','','True','',''])
            # TODO : to check duplicate source_ids    
            else:
                if gb_show_type=='SM' or gb_show_type=='MO':
                    gb_duplicate_api=self.source_duplicate_api%(gb_id,gb_show_type.encode())
                else:
                    #TODO: for episodes
                    gb_duplicate_api=self.source_duplicate_api%(gb_sm_id,'SM')    
                data_GB_resp_duplicate=self.fetch_response_for_id(gb_duplicate_api,self.token)
                if data_GB_resp_duplicate!=[]:
                    for px_id in data_GB_resp_duplicate:
                        duplicate_px_id.append(px_id.get("projectxId"))
                    if gb_show_type=='MO' or gb_show_type=='SM':    
                        source_id_duplicate=self.getting_duplicate_source_id(duplicate_px_id,gb_show_type)
                    else:
                        #TODO : for episode
                        source_id_duplicate=self.getting_duplicate_source_id(duplicate_px_id,'SM')    
                    self.writer.writerow([gb_sm_id,gb_id,gb_show_type,'True',source_id_duplicate,'True','',''])
                else:
                    #TODO: to check whether gb_id present in GB dump itself
                    gb_present_data_checking=self.check_presence_in_DB%(gb_id,gb_show_type.encode())
                    data_GB_resp=self.fetch_response_for_id(gb_present_data_checking,self.token)
                    if data_GB_resp==True:
                        self.writer.writerow([gb_sm_id,gb_id,gb_show_type,'False','','','True','True'])           
                    else:
                        self.writer.writerow([gb_sm_id,gb_id,gb_show_type,'False','','','True','False'])   
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,RuntimeError,ValueError) as e:
            retry_count+=1
            print ("exception caught ingestion_checking func.................................",type(e),gb_id,gb_show_type,thread_name)
            print ("\n") 
            print ("Retrying.............")
            if retry_count<=5:
                self.ingestion_checking(gb_id,gb_sm_id,gb_show_type,thread_name)
            else:
                retry_count=0                

    #TODO: getting source_ids from GB_dump
    def getting_source_ids(self,data,thread_name):
        #import pdb;pdb.set_trace()
        gb_id=data.get("gb_id")
        gb_sm_id=data.get("show_id")
        gb_show_type=data.get("show_type").encode('utf-8')
        if gb_id is not None:
            if gb_show_type=='MO' and gb_id not in self.gb_mo_list:
                self.gb_mo_list.append(gb_id)
            elif gb_show_type=='SM' and gb_id not in self.gb_sm_list:
                self.gb_sm_list.append(gb_id)
            elif gb_show_type=='SE' and gb_id not in self.gb_se_list:
                self.gb_se_list.append(gb_id)
            print("\n")                                                          
            print ({"Total":len(self.gb_mo_list)+len(self.gb_se_list)+len(self.gb_sm_list),"Thread_name":thread_name})
            print datetime.datetime.now()    
            return {"gb_id":gb_id,"gb_sm_id":gb_sm_id,"gb_show_type":gb_show_type,"thread_name":thread_name}
        else:
            return "None"                       

    #TODO: opening file for writing
    def create_csv(result_sheet):
        if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file

    def main(self,start_id,thread_name,end_id,page_id):
        result_sheet='/output/Ingestion_checked_in_Px%d.csv'%page_id
        fieldnames = ["Gb_SM_id","gb_id","gb_show_type","Duplicate_present","Duplicate_source_id","Ingested","Not_ingested","Data_present_in_dump"]
        output_file=open(os.getcwd()+result_sheet,"wa")
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(fieldnames)
            print("Checking ingestion of series and Movies to Projectx search api.............",thread_name)
            for aa in range(start_id,end_id,1000):
                try:
                    #import pdb;pdb.set_trace()
                    GB_query=self.mytable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"$and":[{"show_type":{"$in":["SE","MO","SM"]}}]}},{"$project":{"gb_id":1,"show_id":1,"_id":0,"show_type":1}}]) 
                    print ("\n")
                    print({"start": start_id,"end":end_id})
                    for data in GB_query:
                        details=self.getting_source_ids(data,thread_name) 
                        if details!="None":   
                            self.ingestion_checking(details["gb_id"],details["gb_sm_id"],details["gb_show_type"],details["thread_name"])
                except (Exception,socket.error,pymongo.errors.CursorNotFound) as e:
                    pass            
        output_file.close()      
        self.connection.close()          

    # TODO: Threading Operations to call getting_px_ids
    def thread_pool(self):    
        
        t1=threading.Thread(target=self.main,args=(0,"thread - 1",50000,1))
        t1.start()
        t2=threading.Thread(target=self.main,args=(50030,"thread - 2",100000,2))
        t2.start()                          
        t3=threading.Thread(target=self.main,args=(100000,"thread - 3",150000,3))
        t3.start()
        t4=threading.Thread(target=self.main,args=(150000,"thread - 4",200000,4))
        t4.start()
        t5=threading.Thread(target=self.main,args=(200000,"thread - 5",250000,5))
        t5.start()
        t6=threading.Thread(target=self.main,args=(250000,"thread - 6",300000,6))
        t6.start()
        t7=threading.Thread(target=self.main,args=(300000,"thread - 7",350000,7))
        t7.start()
        t8=threading.Thread(target=self.main,args=(350000,"thread - 8",400000,8))
        t8.start()
        t9=threading.Thread(target=self.main,args=(400000,"thread - 9",450000,9))
        t9.start()
        t10=threading.Thread(target=self.main,args=(450000,"thread - 10",500000,10))
        t10.start()                          
        t11=threading.Thread(target=self.main,args=(500000,"thread - 11",550000,11))
        t11.start()
        t12=threading.Thread(target=self.main,args=(550000,"thread - 12",600000,12))
        t12.start()
        t13=threading.Thread(target=self.main,args=(600000,"thread - 13",650000,13))
        t13.start()
        t14=threading.Thread(target=self.main,args=(650000,"thread - 14",700000,14))
        t14.start()
        t15=threading.Thread(target=self.main,args=(700000,"thread - 15",750000,15))
        t15.start()
        t16=threading.Thread(target=self.main,args=(750000,"thread - 16",800000,16))
        t16.start()
        t17=threading.Thread(target=self.main,args=(800000,"thread - 17",850000,17))
        t17.start()
        t18=threading.Thread(target=self.main,args=(850000,"thread - 18",900000,18))
        t18.start()

#TODO: starting and creating class object and calling functions
object_=GB_ingestion()
object_.__init__()
object_.get_env_url()
object_.mongo_connection()
object_.thread_pool()    
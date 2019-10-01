"""Saayan"""

from multiprocessing import Process
import threading
import pymysql
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
import datetime
import urllib2
import json
import httplib
import unidecode
sys.setrecursionlimit(1500)

class hbonow_ingestion:

    #TODO: INITIALIZATION
    def __init__(self):
        self.hbonow_id=0
        self.hbonow_show_type=''
        self.title=''
        self.season_number=0
        self.episode_number=0
        self.year=''
        self.expired=''
        self.expiry_date=''

    #TODO: one time call param
    def constant_param(self):
        self.source="HBONOW"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.total=0
        self.ingested_count=0
        self.not_ingested_count=0
        self.hbonow_mo_list=[]
        self.hbonow_sm_list=[]
        self.hbonow_se_list=[]    

    def get_env_url(self):    
        self.source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=%s&showType=%s"
        self.source_duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%s&sourceName=%s&showType=%s"
        self.px_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'
    
    # set up connection of DB
    def mysql_connection(self):
        self.connection=pymysql.connect(user="root",passwd="branch@123",host="192.168.86.10",db="branch_service",port=3306)
        self.cur=self.connection.cursor()

    def query_execute(self):
        try:
            self.query="select launch_id,show_type,title,season_number,episode_number,release_year,duration,expired,expired_at from hbonow_programs;"
            self.cur.execute(self.query)
            self.hbonow_data=self.cur.fetchall()
        except (MySQLError,IntegrityError) as e:
            print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            self.query_execute()    
    
    #TODO: fetching response for the given API
    def fetch_response_for_id(self,api,token):
        resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':token}))
        data = resp.read()
        data_resp = json.loads(data)
        return data_resp    

    def getting_duplicate_source_id(self,px_id,show_type):
        retry_count=0
        source_list=[]
        for _id in px_id:
            resp=urllib2.urlopen(urllib2.Request(self.px_mapping_api%_id,None,{'Authorization':self.token}))
            data_resp_mapping=json.loads(resp.read())
            for resp in data_resp_mapping:
                if resp.get("data_source")==self.source and resp.get("sub_type")==show_type and resp.get("type")=='Program':
                    source_list.append(resp.get("sourceId"))
        return source_list

    #TODO: to check hbonow_id ingestion
    def ingestion_checking(self,thread_name):
        #import pdb;pdb.set_trace()
        retry_count=0
        duplicate_px_id=[]
        try:
            hbonow_mapping_api=self.source_mapping_api%(self.hbonow_id,self.source,self.hbonow_show_type.encode())
            data_hbonow_resp=self.fetch_response_for_id(hbonow_mapping_api,self.token)
            if data_hbonow_resp!=[]: 
                self.ingested_count+=1 
                self.writer.writerow([self.hbonow_id,self.hbonow_show_type,self.title,self.season_number,
                                      self.episode_number,self.year,self.duration,self.expired,self.expiry_date,'','','True'])
            # TODO : to check duplicate source_ids    
            elif self.hbonow_show_type=='SM' or self.hbonow_show_type=='MO':
                hbonow_duplicate_api=self.source_duplicate_api%(self.hbonow_id,self.source,self.hbonow_show_type.encode())
            else:
                #TODO: for episodes
                hbonow_duplicate_api=self.source_duplicate_api%(self.hbonow_id,self.source,'SM')    
            data_hbonow_resp_duplicate=self.fetch_response_for_id(hbonow_duplicate_api,self.token)
            #import pdb;pdb.set_trace()
            if data_hbonow_resp_duplicate!=[]:
                self.ingested_count+=1
                for px_id in data_hbonow_resp_duplicate:
                    duplicate_px_id.append(px_id.get("projectxId"))
                if self.hbonow_show_type=='MO' or self.hbonow_show_type=='SM':    
                    source_id_duplicate=self.getting_duplicate_source_id(duplicate_px_id,self.hbonow_show_type)
                else:
                    #TODO : for episode
                    source_id_duplicate=self.getting_duplicate_source_id(duplicate_px_id,'SM')    
                self.writer.writerow([self.hbonow_id,self.hbonow_show_type,self.title,self.season_number,
                                     self.episode_number,self.year,self.duration,self.expired,self.expiry_date,'True',source_id_duplicate,'True'])
            else:
                self.not_ingested_count+=1
                self.writer.writerow([self.hbonow_id,self.hbonow_show_type,self.title,self.season_number,
                                     self.episode_number,self.year,self.duration,self.expired,self.expiry_date,'False','','','True']) 
            print ("\n")
            print ("%s_id:"%self.source,self.hbonow_id,"show_type:",self.hbonow_show_type,thread_name,
                   "title:",self.title,"season_no:",self.season_number,"episode_no:",self.episode_number,"ingested_count:",self.ingested_count,
                   "not_ingested_count:", self.not_ingested_count)                           
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,RuntimeError,ValueError) as e:
            #import pdb;pdb.set_trace()
            retry_count+=1
            print ("exception caught ingestion_checking func.............................",type(e),self.hbonow_id,self.hbonow_show_type,thread_name)
            print ("\n") 
            print ("Retrying.............")
            if retry_count<=5:
                self.ingestion_checking(thread_name)
            else:
                retry_count=0                

    #TODO: getting source_ids from Showtimeanytime_dump
    def getting_source_details(self,start_id,thread_name,end_id,id_):
        print("Checking ingestion of series and Movies to Projectx search api.............",thread_name)
        #import pdb;pdb.set_trace()
        self.hbonow_id=(self.hbonow_data[id_])[0]
        self.hbonow_show_type=(self.hbonow_data[id_])[1]
        self.title=(self.hbonow_data[id_])[2]
        self.season_number=(self.hbonow_data[id_])[3]
        self.episode_number=(self.hbonow_data[id_])[4]
        self.year=(self.hbonow_data[id_])[5]
        self.duration=(self.hbonow_data[id_])[6]
        self.expired=(self.hbonow_data[id_])[7]
        self.expiry_date=(self.hbonow_data[id_])[8]
        if self.hbonow_id is not None:
            if self.hbonow_show_type=='movie' and self.hbonow_id not in self.hbonow_mo_list:
                self.hbonow_mo_list.append(self.hbonow_id)
                self.hbonow_show_type='MO'
            elif self.hbonow_show_type=='tv_show' and self.hbonow_id not in self.hbonow_sm_list:
                self.hbonow_sm_list.append(self.hbonow_id)
                self.hbonow_show_type='SM'
            elif self.hbonow_show_type=='episode' and self.hbonow_id not in self.hbonow_se_list:
                self.hbonow_se_list.append(self.hbonow_id)
                self.hbonow_show_type='SE'

    #TODO: opening file for writing
    def create_csv(self,result_sheet):
        if (os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file        

    #TODO: opening file for writing
    def main(self,start_id,thread_name,end_id,page_id):
        #import pdb;pdb.set_trace()
        self.constant_param()
        result_sheet='/output/Ingestion_checked_in_Px%d.csv'%page_id
        fieldnames = ["%s_id"%self.source,"%s_show_type"%self.source,"title","season_number","episode_number","year",
                      "Duration","expired","expiry_date","Duplicate_present","Duplicate_source_id","Ingested","Not_ingested"]
        output_file=self.create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(fieldnames)
            for id_ in range(start_id,end_id):
                self.total+=1
                self.__init__()
                self.getting_source_details(start_id,thread_name,end_id,id_)
                self.ingestion_checking(thread_name)
                print("\n")                                                          
                print ({"Total":self.total,"ingested_count":self.ingested_count,
                       "not_ingested_count": self.not_ingested_count,"Thread_name":thread_name})
                print("\n")
                print datetime.datetime.now()                       
        output_file.close() 

    # TODO: multi process Operations to call getting_px_ids
    def thread_pool(self): 
        t1=Process(target=self.main,args=(0,"thread - 1",1000,1))
        t1.start()
        t2=Process(target=self.main,args=(1000,"thread - 2",2000,2))
        t2.start()
        t3=Process(target=self.main,args=(2000,"thread - 3",3000,3))
        t3.start()
        t4=Process(target=self.main,args=(3000,"thread - 4",4000,4))
        t4.start()
        t5=Process(target=self.main,args=(4000,"thread - 5",5000,5))
        t5.start()
        t6=Process(target=self.main,args=(5000,"thread - 6",6000,6))
        t6.start()
        t7=Process(target=self.main,args=(6000,"thread - 7",7000,7))
        t7.start()
        self.connection.close()

#TODO: starting and creating class object and calling functions
object_=hbonow_ingestion()
object_.__init__()
object_.get_env_url()
object_.mysql_connection()
object_.query_execute()
object_.thread_pool()    
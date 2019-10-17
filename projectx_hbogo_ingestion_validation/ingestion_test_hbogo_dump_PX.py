"""Saayan"""

from multiprocessing import Process
import logging
import pymysql
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
import datetime
from datetime import datetime
import urllib2
import json
import httplib
import unidecode
sys.setrecursionlimit(1500)

class hbogo_ingestion:

    #TODO: INITIALIZATION
    def __init__(self):
        self.hbogo_id=0
        self.hbogo_show_type=''
        self.title=''
        self.series_title=''
        self.season_number=0
        self.episode_number=0
        self.year=''
        self.expired=''
        self.expiry_date=''
        self.duration_flag=''
        self.updated_at=''

    #TODO: one time call param
    def constant_param(self):
        self.source="HBOGO"
        self.logger=''
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.total=0
        self.ingested_count=0
        self.not_ingested_count=0
        self.hbogo_mo_list=[]
        self.hbogo_sm_list=[]
        self.hbogo_se_list=[] 
        self.hbogo_sn_list=[]   

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
            self.query="select launch_id,show_type,title,series_title,season_number,episode_number,release_year,duration,expired,expired_at,updated_at from hbogo_programs;"
            self.cur.execute(self.query)
            self.hbogo_data=self.cur.fetchall()
        except (MySQLError,IntegrityError) as e:
            self.logger.debug(['Got error {!r}, errno is {}'.format(e, e.args[0])])
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

    #TODO: to check hbogo_id ingestion
    def ingestion_checking(self,thread_name):
        #import pdb;pdb.set_trace()
        retry_count=0
        duplicate_px_id=[]
        try:
            hbogo_mapping_api=self.source_mapping_api%(self.hbogo_id,self.source,self.hbogo_show_type.encode())
            data_hbogo_resp=self.fetch_response_for_id(hbogo_mapping_api,self.token)
            if data_hbogo_resp!=[]: 
                self.ingested_count+=1 
                self.writer.writerow([self.hbogo_id,self.hbogo_show_type,self.title,self.series_title,self.season_number,
                                      self.episode_number,self.year,self.duration,self.duration_flag,self.updated_at,self.expired,self.expiry_date,
                                                                                                         '','','True'])
            # TODO : to check duplicate source_ids    
            elif self.hbogo_show_type=='SM' or self.hbogo_show_type=='MO':
                hbogo_duplicate_api=self.source_duplicate_api%(self.hbogo_id,self.source,self.hbogo_show_type.encode())
            else:
                #TODO: for episodes
                hbogo_duplicate_api=self.source_duplicate_api%(self.hbogo_id,self.source,'SM')    
            data_hbogo_resp_duplicate=self.fetch_response_for_id(hbogo_duplicate_api,self.token)
            #import pdb;pdb.set_trace()
            if data_hbogo_resp_duplicate!=[]:
                self.ingested_count+=1
                for px_id in data_hbogo_resp_duplicate:
                    duplicate_px_id.append(px_id.get("projectxId"))
                if self.hbogo_show_type=='MO' or self.hbogo_show_type=='SM':    
                    source_id_duplicate=self.getting_duplicate_source_id(duplicate_px_id,self.hbogo_show_type)
                else:
                    #TODO : for episode
                    source_id_duplicate=self.getting_duplicate_source_id(duplicate_px_id,'SM')    
                self.writer.writerow([self.hbogo_id,self.hbogo_show_type,self.title,self.series_title,self.season_number,
                                     self.episode_number,self.year,self.duration,self.duration_flag,self.updated_at,self.expired,self.expiry_date,
                                                                                                          'True',source_id_duplicate,'True'])
            else:
                self.not_ingested_count+=1
                self.writer.writerow([self.hbogo_id,self.hbogo_show_type,self.title,self.series_title,self.season_number,
                                     self.episode_number,self.year,self.duration,self.duration_flag,self.updated_at,self.expired,self.expiry_date,'False','','','True']) 
            self.logger.debug('\n')
            self.logger.debug(["%s_id:"%self.source,self.hbogo_id,"show_type:",self.hbogo_show_type,thread_name,"updated:",self.updated_at.strftime("%m/%d/%Y %H:%M:%S"),
                   "title:",self.title,"series_title: ",self.series_title,"season_no:",self.season_number,"episode_no:",self.episode_number,"ingested_count:",self.ingested_count,
                   "not_ingested_count:", self.not_ingested_count])                              
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,RuntimeError,ValueError) as e:
            #import pdb;pdb.set_trace()
            retry_count+=1
            self.logger.debug(["exception caught ingestion_checking func.........................",type(e),self.hbogo_id,self.hbogo_show_type,thread_name])
            self.logger.debug(["Retrying............."])
            if retry_count<=5:
                self.ingestion_checking(thread_name)
            else:
                retry_count=0 

    def duration_checking_less_than_sixty_mins(self,duration):
        # import pdb;pdb.set_trace()
        if duration is not None and duration!='NULL':
            if "hr" in duration.split(" "):
                self.duration_flag="True"
            elif "min" in duration.split(" "):
                if eval(duration.split("min")[0].strip()) > 60:
                    self.duration_flag="True"
                else:
                    self.duration_flag="False"
            else:
                self.duration_flag="NULL"                               
        else:
            self.duration_flag="NULL"            

    #TODO: getting source_ids from Showtimeanytime_dump
    def getting_source_details(self,start_id,thread_name,end_id,id_):
        self.logger.debug("\n")
        self.logger.debug(["Checking ingestion of hbogo series and Movies to Projectx search api.............",thread_name])
        #import pdb;pdb.set_trace()
        self.hbogo_id=(self.hbogo_data[id_])[0]
        self.hbogo_show_type=(self.hbogo_data[id_])[1]
        self.title=(self.hbogo_data[id_])[2]
        self.series_title=(self.hbogo_data[id_])[3]
        self.season_number=(self.hbogo_data[id_])[4]
        self.episode_number=(self.hbogo_data[id_])[5]
        self.year=(self.hbogo_data[id_])[6]
        self.duration=(self.hbogo_data[id_])[7]
        self.duration_checking_less_than_sixty_mins(self.duration)
        self.expired=(self.hbogo_data[id_])[8]
        self.expiry_date=(self.hbogo_data[id_])[9]
        self.updated_at=(self.hbogo_data[id_])[10]
        if self.hbogo_id is not None:
            if self.hbogo_show_type=='MO' and self.hbogo_id not in self.hbogo_mo_list:
                self.hbogo_mo_list.append(self.hbogo_id)
            elif self.hbogo_show_type=='SM' and self.hbogo_id not in self.hbogo_sm_list:
                self.hbogo_sm_list.append(self.hbogo_id)
            elif self.hbogo_show_type=='SE' and self.hbogo_id not in self.hbogo_se_list:
                self.hbogo_se_list.append(self.hbogo_id)
            else:
                self.hbogo_sn_list.append(self.hbogo_id)     
        self.logger.debug("\n")
        self.logger.debug (["total:",len(self.hbogo_mo_list)+len(self.hbogo_sm_list)+len(self.hbogo_se_list)+len(self.hbogo_sn_list),thread_name])       

    #TODO: opening file for writing
    def create_csv(self,result_sheet):
        #import pdb;pdb.set_trace()
        if (os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file

    def create_log(self,log_file):
        #import pdb;pdb.set_trace()
        self.logger = logging.getLogger()
        logging.basicConfig(filename=log_file,format=[],filemode='wa')
        self.logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(stream_handler)
    
    def main(self,start_id,thread_name,end_id,page_id):
        #import pdb;pdb.set_trace()
        self.constant_param()
        self.create_log(os.getcwd()+"/log/log.txt")
        result_sheet='/output/%s_Ingestion_checked_in_Px%d.csv'%(self.source,page_id)
        fieldnames = ["%s_id"%self.source,"%s_show_type"%self.source,"title","series_title","season_number","episode_number","year",
                      "Duration","Duration > 60 mins","Updated_at","expired","expiry_date","Duplicate_present","Duplicate_source_id",
                       "Ingested","Not_ingested"]
        output_file=self.create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(fieldnames)
            for id_ in range(start_id,end_id):
                self.total+=1
                self.__init__()
                self.getting_source_details(start_id,thread_name,end_id,id_) 
                if self.hbogo_show_type is not None:   
                    self.ingestion_checking(thread_name)
                else:
                    self.writer.writerow([self.hbogo_id,self.hbogo_show_type,self.title,self.series_title,self.season_number,
                                         self.episode_number,self.year,self.duration,self.duration_flag,self.updated_at,self.expired,self.expiry_date])         
                self.logger.debug("\n")
                self.logger.debug([{"Total":self.total,"ingested_count":self.ingested_count,
                       "not_ingested_count": self.not_ingested_count,"Thread_name":thread_name}])
                self.logger.debug("\n")
                self.logger.debug(["date time:", datetime.now().strftime("%m/%d/%Y %H:%M:%S")])
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
        t7=Process(target=self.main,args=(6000,"thread - 7",6884,7))
        t7.start()
        self.connection.close()

#TODO: starting and creating class object and calling functions
object_=hbogo_ingestion()
object_.__init__()
object_.get_env_url()
object_.mysql_connection()
object_.query_execute() 
object_.thread_pool()    

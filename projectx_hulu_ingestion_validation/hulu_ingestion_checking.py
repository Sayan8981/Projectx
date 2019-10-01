"""Writer: Saayan"""
import threading
from multiprocessing import Process
import sys
import csv
import pymongo
import datetime
import urllib2
import json
import os
from urllib2 import URLError,HTTPError
import httplib
import socket 
import pinyin
import unidecode

class hulu_ingestion:

    def __init__(self):
        self.source='Hulu'
        self.not_ingested_count=0
        self.multiple_mapped_count=0
        self.pass_count=0
        self.arr_hulu_se=[]
        self.arr_hulu_sm=[]
        self.total_hulu_id_SM=0
        self.total_hulu_id_SE=0  
        self.total_hulu_id_MO=0  
        self.hulu_id_sm=0
        self.hulu_id_se=0
        self.show_type='' 
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.fieldnames = ["%s_id"%self.source,"show_type","title","release_year","series_title","episode_number","video_language","projectx_id_%s"%self.source,
                       "Comment","duplicate_response_present","duplicate_%s_id"%self.source,"type","series_type","px_response","duration"]
    
    def variable_param(self):
        self.title=''
        self.px_response='True'
        self.release_year=0
        self.series_title=''
        self.episode_number=''
        self.series_type=''
        self.type=''
        self.duration=''
        self.video_language=''             

    def get_env_url(self):
        self.source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Hulu&showType=%s"
        self.duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%s&sourceName=%s&showType=%s'
        self.px_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'
        self.projectx_preprod_api='https://preprod.caavo.com/programs?ids=%d&ott=true&aliases=true'

    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
        self.mydb=self.connection["qadb"]
        self.idtable_movies=self.mydb["HuluValidMovies"] 
        self.idtable_episodes=self.mydb["HuluValidEpisodes"] 

    def create_csv(self,result_sheet):
        #import pdb;pdb.set_trace()
        if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file  

    def query_huluepisode(self,id):
        query_huluepisode=self.idtable_episodes.aggregate([{"$match":{"programming_type":"Full Episode"}},{"$project":{"id":1,"_id":0,"title":1,"programming_type":1,"type":1,"site_id":1,"series.name":1,"series.site_id":1,"series.original_premiere_date":1,"original_premiere_date":1,"number":1,"series.type":1,"duration":1,"video_language":1}},{"$skip":id},{"$limit":1000}])   
        return query_huluepisode

    def query_hulumovie(self,id):
        query_hulumovie=self.idtable_movies.aggregate([{"$skip":id},{"$limit":1000},{"$match":{"programming_type":"Full Movie"}},{"$project":{"id":1,"_id":0,"title":1,"programming_type":1,"type":1,"site_id":1,"original_premiere_date":1,"duration":1,"video_language":1}}])    
        return query_hulumovie

    #TODO: fetching response for API
    def fetch_from_cloud_for_id(self,api):
        #import pdb;pdb.set_trace()
        resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':self.token}))
        data_resp = json.loads(resp.read())
        return data_resp

    #TODO: to get duplicate source ids from mapping API
    def getting_duplicate_source_id(self,px_id,px_mapping_api,show_type):
        source_list=[]
        #import pdb;pdb.set_trace()
        for _id in px_id:
            resp=urllib2.urlopen(urllib2.Request(self.px_mapping_api%_id,None,{'Authorization':self.token}))
            data_=resp.read()
            data_resp_mapping=json.loads(data_)

            for resp in data_resp_mapping:
                if resp.get("data_source")==self.source and resp.get("sub_type")==show_type and resp.get("type")=='Program':
                    source_list.append(resp.get("sourceId"))
        return source_list        

    #TODO: to validate the ingestion and printing to O/P sheet 
    def validation_result(self,_id,_projectx_id,show_type,writer,thread_name):
        #import pdb;pdb.set_trace()
        retry_count=0
        duplicate_present=''
        duplicate_px_id=[]
        try:
            if _projectx_id:
                if len(_projectx_id)>1:
                    self.multiple_mapped_count+=1
                    writer.writerow({"%s_id"%self.source:_id,"show_type":show_type,"title":self.title,"release_year":self.release_year,"series_title":self.series_title,
                            "episode_number":self.episode_number,"video_language":self.video_language,"projectx_id_%s"%self.source:_projectx_id,"Comment":'Multiple ingestion for same content of ',
                                                                                           "type":self.type,"series_type":self.series_type,"duration":self.duration})
                elif len(_projectx_id)==1:
                    preprod_resp_api=self.projectx_preprod_api%(_projectx_id[0])
                    data_resp=self.fetch_from_cloud_for_id(preprod_resp_api)
                    if data_resp==[]:
                        self.px_response='Null'
                    self.pass_count+=1
                    writer.writerow({"%s_id"%self.source:_id,"show_type":show_type,"title":self.title,"release_year":self.release_year,"series_title":self.series_title,
                                           "episode_number":self.episode_number,"video_language":self.video_language,"projectx_id_%s"%self.source:_projectx_id,"Comment":'Ingested',"type":self.type,
                                                          "series_type":self.series_type,"px_response":self.px_response,"duration":self.duration})
            else:
                data_response_duplicate=self.fetch_from_cloud_for_id(self.duplicate_api%(_id,self.source,show_type))
                if data_response_duplicate!=[]:
                    duplicate_present='True'
                    for px_id in data_response_duplicate:
                        duplicate_px_id.append(px_id.get("projectxId"))
                    source_id_duplicate=self.getting_duplicate_source_id(duplicate_px_id,self.px_mapping_api,show_type)    
                    writer.writerow({"%s_id"%self.source:_id,"show_type":show_type,"title":self.title,"release_year":self.release_year,"series_title":self.series_title,
                                 "episode_number":self.episode_number,"video_language":self.video_language,"projectx_id_%s"%self.source:'',"Comment":'Not Ingested',"duplicate_response_present":duplicate_present,
                                                    "duplicate_%s_id"%self.source:source_id_duplicate,"type":self.type,"series_type":self.series_type,"duration":self.duration})
                else:    
                    self.not_ingested_count+=1
                    writer.writerow({"%s_id"%self.source:_id,"show_type":show_type,"title":self.title,"release_year":self.release_year,"series_title":self.series_title,"episode_number":self.episode_number,"video_language":self.video_language,"projectx_id_%s"%self.source:'',"Comment":'Not Ingested',"type":self.type,"series_type":self.series_type,"duration":self.duration})        

            print ("\n") 
            print("hulu_id:",_id,"thread name:", thread_name,"Not ingested: ", self.not_ingested_count, "Multiple mapped content :", self.multiple_mapped_count,
                                                                "Total Fail: ", self.not_ingested_count+self.multiple_mapped_count, "Pass: ", self.pass_count)  
            print ("\n")
            print(datetime.datetime.now())        
        except (Exception,HTTPError,urllib2.URLError) as e:
            retry_count+=0
            print ("Exception caught........................",type(e),_id,_projectx_id,show_type,thread_name)
            print("Retrying...................",retry_count)
            if retry_count<=5:
                self.validation_result(_id,_projectx_id,show_type,writer,thread_name)       
            else:
                retry_count=0    

    #TODO: getting px_ids form source_mapping API
    def getting_px_ids(self,_id,show_type):            
        #import pdb;pdb.set_trace()
        retry_count=0
        projectx_id=[]
        try:
            data_response_api=self.fetch_from_cloud_for_id(self.source_mapping_api%(_id,show_type))
            if data_response_api:
                for data in data_response_api:
                    if data["data_source"]==self.source and data["type"]=="Program" and data["sub_type"]==show_type:
                        projectx_id.append(data["projectxId"])
                        return projectx_id 
            else:
                return projectx_id                               
        except (Exception,HTTPError,urllib2.URLError,socket.error) as e:
            retry_count+=1
            print ("Exception caught............",type(e),_id,show_type)
            print ("Retrying........",retry_count)
            if retry_count<=5:
                self.getting_px_ids(_id,show_type)            
            else:
                retry_count=0    

    # TODO: Ingestion checking only for hulu_movies
    def ingestion_checking(self,hulu_id,show_type,writer,thread_name):
        #import pdb;pdb.set_trace()
        retry_count=0
        try:
            px_id=self.getting_px_ids(hulu_id,show_type)     
            self.validation_result(hulu_id,px_id,show_type,writer,thread_name)    
        except (Exception,HTTPError,urllib2.URLError,socket.error) as e:
            retry_count+=1
            print ("Exception caught............",type(e),hulu_id,show_type)
            print ("Retrying........",retry_count)
            if retry_count<=5:
                self.ingestion_checking(hulu_id,show_type,writer,thread_name)            
            else:
                retry_count=0             

    def main_func_episodes(self,start_id,thread_name,end_id,page_id):
        #import pdb;pdb.set_trace()
        print({"start":start_id,"end":end_id})    
        result_sheet='/output/No_ingestion_test_SE_%s_id%d.csv'%(self.source,page_id)
        output_file=self.create_csv(result_sheet)
        with output_file as mycsvfile:
            writer = csv.DictWriter(mycsvfile,fieldnames=self.fieldnames,dialect="excel",lineterminator = '\n')
            writer.writeheader()
            for aa in range(start_id,end_id,1000):
                try:
                    query_huluepisode=self.query_huluepisode(aa)
                except (Exception,pymongo.errors.OperationFailure,pymongo.errors.CursorNotFound,RuntimeError) as e:
                    print ("Exception in query...........",type(e),thread_name)
                    query_huluepisode=self.query_huluepisode(aa)    
                    #import pdb;pdb.set_trace()
                for data in query_huluepisode:
                    self.variable_param()
                    self.series_title=unidecode.unidecode(pinyin.get(data.get("series").get("name")))
                    self.series_type=data.get("series").get("type")
                    self.type=data.get("type")
                    self.duration=data.get("duration")
                    self.video_language=data.get("video_language")
                    if data.get("site_id") is not None:
                        self.hulu_id_se=data.get("site_id")
                    if data.get("series").get("site_id") is not None:    
                        self.hulu_id_sm=data.get("series").get("site_id")
                    if self.hulu_id_sm not in self.arr_hulu_sm and self.hulu_id_sm!=0:
                        self.arr_hulu_sm.append(self.hulu_id_sm)
                        self.show_type='SM'
                        self.total_hulu_id_SM+=1
                        self.release_year=data.get("series").get("original_premiere_date")
                        self.ingestion_checking(self.hulu_id_sm,self.show_type,writer,thread_name)
                    if self.hulu_id_se not in self.arr_hulu_se and self.hulu_id_se!=0:   
                        self.title=unidecode.unidecode(pinyin.get(data.get("title"))) 
                        self.arr_hulu_se.append(self.hulu_id_se)
                        self.show_type='SE'
                        self.release_year=data.get("original_premiere_date")
                        self.episode_number=data.get("number")
                        self.total_hulu_id_SE+=1
                        self.ingestion_checking(self.hulu_id_se,self.show_type,writer,thread_name)
                    print ("\n") 
                    print ({"total_hulu_id_SE":self.total_hulu_id_SE,"total_hulu_id_SM":self.total_hulu_id_SM,"hulu_id_se":self.hulu_id_se,"hulu_id_sm": self.hulu_id_sm})      
        output_file.close()            

    #TODO: to open file for writing output
    def main_func_movies(self,start_id,thread_name,end_id,page_id):
        #import pdb;pdb.set_trace()
        print({"start":start_id,"end":end_id})    
        result_sheet='/output/No_ingestion_test_MO_%s_id%d.csv'%(self.source,page_id)
        output_file=self.create_csv(result_sheet)
        with output_file as mycsvfile:
            writer = csv.DictWriter(mycsvfile,fieldnames=self.fieldnames,dialect="excel",lineterminator = '\n')
            writer.writeheader()
            for aa in range(start_id,end_id,1000):
                try:
                    query_hulumovie=self.query_hulumovie(aa)
                except (Exception,pymongo.errors.OperationFailure,pymongo.errors.CursorNotFound,RuntimeError) as e:
                    print ("Exception in query...........",type(e),thread_name)
                    query_hulumovie=self.query_hulumovie(aa)
                for data in query_hulumovie:
                    self.variable_param()
                    if data.get("site_id") is not None:
                        hulu_id_mo=data.get("site_id")
                        self.total_hulu_id_MO+=1 
                        #import pdb;pdb.set_trace()
                        self.show_type='MO'
                        self.title=unidecode.unidecode(pinyin.get(unicode(data.get("title"))))
                        self.release_year=data.get("original_premiere_date").split("-")[0]
                        self.type=data.get("type")
                        self.duration=data.get("duration")
                        self.video_language=data.get("video_language")
                        print ("\n")
                        print ({"total":self.total_hulu_id_MO,"thread_name":thread_name})
                        self.ingestion_checking(hulu_id_mo,self.show_type,writer,thread_name)           
        output_file.close()
            
    # TODO: to create  and run threads
    def threading_pool(self):
        
        t1=Process(target=self.main_func_episodes,args=(0,"thread-1",20000,1))
        t1.start()
        t2=Process(target=self.main_func_episodes,args=(20000,"thread-2",40000,2))
        t2.start()
        t3=Process(target=self.main_func_episodes,args=(40000,"thread-3",60000,3))
        t3.start()
        t4=Process(target=self.main_func_episodes,args=(60000,"thread-4",100000,4))
        t4.start()
        t5=Process(target=self.main_func_movies,args=(0,"thread-5",2000,5))
        t5.start()

        self.connection.close() 


object_ingestion=hulu_ingestion()
object_ingestion.__init__()
object_ingestion.get_env_url()
object_ingestion.mongo_connection()                
object_ingestion.threading_pool()
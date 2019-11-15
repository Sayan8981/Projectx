"""Sdatayan"""

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
import unidecode
import pinyin
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500)

class hulu_ids:
    #TODO: INITIALIZATION
    def __init__(self):
        self.source='Hulu'
        self.total_se=0
        self.total_mo=0
        self.arr_hulu_se=[]
        self.arr_hulu_sm=[]
        self.idtable_movies='' 
        self.idtable_episodes=''

    def variable_param(self):
        self.title=''
        self.release_year=0
        self.series_title=''
        self.episode_number=''
        self.series_type=''
        self.type=''
        self.duration=''
        self.video_language='' 
        self.hulu_id_sm=0
        self.hulu_id_se=0
        self.hulu_id_mo=0
        self.show_type='' 

    def mongo_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
        self.mydb=self.connection["qadb"]
        self.idtable_movies=self.mydb["HuluValidMovies"] 
        self.idtable_episodes=self.mydb["HuluValidEpisodes"]      

    def query_huluepisode(self,id):
        query_huluepisode=self.idtable_episodes.aggregate([{"$match":{"programming_type":"Full Episode"}},{"$project":{"id":1,"_id":0,"title":1,"programming_type":1,"type":1,"site_id":1,"series.name":1,"series.site_id":1,"series.original_premiere_date":1,"original_premiere_date":1,"number":1,"series.type":1,"duration":1,"video_language":1,"availability.svod.end":1}},{"$skip":id},{"$limit":1000}])   
        return query_huluepisode

    def query_hulumovie(self,id):
        query_hulumovie=self.idtable_movies.aggregate([{"$match":{"programming_type":"Full Movie"}},{"$project":{"id":1,"_id":0,"title":1,"programming_type":1,"type":1,"site_id":1,"original_premiere_date":1,"duration":1,"video_language":1,"availability.svod.end":1}},{"$skip":id},{"$limit":1000}])    
        return query_hulumovie  

    def getting_source_movies_details(self,data):
        self.variable_param()
        if data.get("site_id") is not None:
            self.hulu_id_mo=data.get("site_id")
            #import pdb;pdb.set_trace()
            self.show_type='MO'
            self.title=unidecode.unidecode(pinyin.get(unicode(data.get("title"))))
            self.release_year=data.get("original_premiere_date").split("-")[0]
            self.type=data.get("type")
            self.duration=data.get("duration")
            self.video_language=data.get("video_language")
            print ("\n")
            print({"series_id":self.hulu_id_sm,"_id":self.hulu_id_mo,"show_type":self.show_type,
            "Video_language":self.video_language,"series_type":self.series_type,"episode_type":self.type,
            "duration":self.duration})
            return {"series_id":self.hulu_id_sm,"_id":self.hulu_id_mo,"show_type":self.show_type,
            "Video_language":self.video_language,"series_type":'Null',"episode_type":self.type,
            "duration":self.duration}

    def getting_source_episode_details(self,data): 
        #import pdb;pdb.set_trace()
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
            self.release_year=data.get("series").get("original_premiere_date")
        if self.hulu_id_se not in self.arr_hulu_se and self.hulu_id_se!=0:    
            self.title=unidecode.unidecode(pinyin.get(data.get("title"))) 
            self.arr_hulu_se.append(self.hulu_id_se) 
            self.show_type='SE'
            self.release_year=data.get("original_premiere_date")
            self.episode_number=data.get("number")
        print ("\n")    
        print({"series_id":self.hulu_id_sm,"_id":self.hulu_id_se,"show_type":self.show_type,
            "Video_language":self.video_language,"series_type":self.series_type,"episode_type":self.type,
            "duration":self.duration})    
        return {"series_id":self.hulu_id_sm,"_id":self.hulu_id_se,"show_type":self.show_type,
            "Video_language":self.video_language,"series_type":self.series_type,"episode_type":self.type,
            "duration":self.duration}                     

    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.mongo_connection()
        print({"start":start_id,"end":end_id}) 
        fieldnames = ["series_id","_id","show_type","Video_language","series_type","type","duration"]   
        result_sheet='/output_hulu/%s_id_%s_%s.csv'%(self.source,thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)

        with output_file as mycsvfile:
            writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            writer.writerow(fieldnames)
            #import pdb;pdb.set_trace()
            if end_id>2000:
                for data in range(start_id,end_id,1000):
                    id_query_episodes=self.query_huluepisode(data)
                    for data_ in id_query_episodes:
                    	self.total_se+=1
                    	print("\n Total_SE:",self.total_se)
                        if data_.get("availability").get("svod").get("end") is None or data_.get("availability").get("svod").get("end") > datetime.datetime.now().isoformat():
                            details=self.getting_source_episode_details(data_)
                            writer.writerow([details["series_id"],details["_id"],details["show_type"],
                            details["Video_language"],details["series_type"],details["episode_type"],
                            details["duration"]]) 
            else:
                for data in range(start_id,end_id,1000):
                    id_query_movies=  self.query_hulumovie(data) 
                    for data_ in id_query_movies:
                    	self.total_mo+=1
                    	print("\n Total_MO:",self.total_mo)
                        if data_.get("availability").get("svod").get("end") is None or data_.get("availability").get("svod").get("end") > datetime.datetime.now().isoformat():                 
                            details=self.getting_source_movies_details(data_)
                            writer.writerow([details["series_id"],details["_id"],details["show_type"],
                            details["Video_language"],details["series_type"],details["episode_type"],
                            details["duration"]])
    
        output_file.close()
        self.connection.close()

    # TODO : to run threads in Thread pool
    def thread_pool(self):   

        t1=Process(target=self.main,args=(0,"thread-1",20000))
        t1.start()
        t2=Process(target=self.main,args=(20000,"thread-2",40000))
        t2.start()
        t3=Process(target=self.main,args=(40000,"thread-3",60000))
        t3.start()
        t4=Process(target=self.main,args=(60000,"thread-4",100000))
        t4.start()
        t5=Process(target=self.main,args=(0,"thread-5",2000))
        t5.start()


#starting and calling functions 
object_=hulu_ids()
object_.thread_pool()    
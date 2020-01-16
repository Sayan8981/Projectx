"""Writer: Saayan"""

from multiprocessing import Process
import threading
import csv
import pymongo
import MySQLdb
import datetime
import sys
import urllib2
import json
import os
from urllib2 import URLError,HTTPError
import httplib
import socket
import pinyin
import unidecode
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules,ott_meta_data_validation_modules
sys.setrecursionlimit(1500) 


class meta_data:
    retry_count=0

    def getting_images(self,source_id,show_type,source_show_id,details,sourcetable):
        #import pdb;pdb.set_trace()
        try:
            images_details=[]
            images_details.append({'url':details.get("image")})
                #source_images_url.append(image.get("image"))

            if show_type=='SE' and (images_details==[] or images_details):
                images_query=sourcetable.find({"show_type":'SM',"launch_id":source_show_id},{"image":1,"_id":0})
                        
                for images in images_query:
                    images_details.append({'url':images.get("image")})
                    #source_images_url.append(image.get("url"))

            return images_details

        except (Exception,RuntimeError):
            print ("exception caught getting_images..............",source_id,show_type,source_show_id)    
            self.getting_images(source_id,show_type,source_show_id,details)

    def getting_credits(self,details):
        #import pdb;pdb.set_trace()
        cast_details=[]
        if details.get("credit_summary")!=[] and details.get("credit_summary") is not None:
            
            for cc in details.get("credit_summary"):
                if unidecode.unidecode(pinyin.get(cc.keys()[0]))!='Executive Producer' or unidecode.unidecode(pinyin.get(cc.keys()[0]))!="Producer":
                    cast_details.append(unidecode.unidecode(pinyin.get(cc.get(cc.keys()[0]))))

        return cast_details

    def getting_source_details(self,vudu_id,show_type,source,thread_name,details,sourcetable):
        #import pdb;pdb.set_trace()
        vudu_credit_present='Null'
        vudu_genres=[]
        vudu_duration=''
        vudu_title=''
        vudu_description=''
        vudu_alternate_titles=[]
        vudu_release_year=0
        vudu_link_present='Null'
        try:
            vudu_credits=self.getting_credits(details)
            if vudu_credits:
                vudu_credit_present='True'
            try:        
                vudu_title=unidecode.unidecode(pinyin.get(details.get("title")))
            except TypeError:
                pass
            try:        
                vudu_description=pinyin.get(details.get("description"))
            except TypeError:
                pass

            vudu_show_id=details.get("series_id")

            if details.get("category"):
                for genres in details.get("category"):
                    vudu_genres.append(genres.lower())
            try:
                vudu_alternate_titles=pinyin.get(details.get("alternate_titles"))
            except Exception:
                pass
            vudu_link=details.get("url")
            if vudu_link!="" or vudu_link is not None :
                vudu_link_present='True' 
            try:
                vudu_release_year=eval(details.get("release_year"))
            except Exception:
                pass    
            #import pdb;pdb.set_trace()
            vudu_duration=details.get("duration")
            if vudu_duration is None or vudu_duration==0:
                vudu_duration=0

            #import pdb;pdb.set_trace()
            vudu_episode_number=details.get("episode_number")
            vudu_season_number=details.get("season_number")

            vudu_images_details=self.getting_images(vudu_id,show_type,vudu_show_id,details,sourcetable)

            return {"source_credits":vudu_credits,"source_credit_present":vudu_credit_present,"source_title":vudu_title,"source_description":vudu_description,
            "source_genres":vudu_genres,"source_alternate_titles":vudu_alternate_titles,"source_release_year":vudu_release_year,"source_duration":vudu_duration,
            "source_season_number":vudu_season_number,"source_episode_number":vudu_episode_number,"source_link_present":vudu_link_present,"source_images_details":vudu_images_details}
            
        except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
            self.retry_count+=1
            print ("exception caught getting_source_details func..............",vudu_id,show_type,source,thread_name)
            print ("\n") 
            print ("Retrying.............",self.retry_count)
            print ("\n")    
            if self.retry_count<=5:
                self.getting_source_details(vudu_id,show_type,source,thread_name,details,sourcetable)    
            else:
                self.retry_count=0

#main class
class mata_data_validation_vudu:

    def __init__(self):
        self.source="Vudu"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.total=0
        self.count_vudu_id=0
        self.writer=''
        self.link_expired=''

    def mongo_mysql_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
        self.sourceDB=self.connection["qadb"] 
        self.sourcetable=self.sourceDB["vududump"]
        self.connection_pxmapping=MySQLdb.Connection(host='192.168.86.10', port=3306, user='root', passwd='branch@123', db='testDB')
        self.px_mappingdb_cur=self.connection_pxmapping.cursor()


    def get_env_url(self):
        self.prod_domain="api.caavo.com"
        self.expired_api='https://%s/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://54.175.96.97:81/projectx/mappingfromsource?sourceIds=%d&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://projectx.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api='http://54.175.96.97:81/projectx/%d/mapping/'
        self.projectx_duplicate_api='http://54.175.96.97:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'

    #TODO: Validation
    def meta_data_validation_(self,data,projectx_id,source_id,show_type,thread_name,only_mapped_ids):
        #import pdb;pdb.set_trace()
        source_details=meta_data().getting_source_details(source_id,show_type,self.source,thread_name,data,self.sourcetable)
        projectx_details=ott_meta_data_validation_modules().getting_projectx_details(projectx_id,show_type,
                                    self.source,thread_name,self.projectx_programs_api,self.token)

        meta_data_validation_result=ott_meta_data_validation_modules().meta_data_validate_vudu().meta_data_validation(source_id,source_details,projectx_details,show_type)

        credits_validation_result=ott_meta_data_validation_modules().credits_validation(source_details,projectx_details)

        images_validation_result=ott_meta_data_validation_modules().images_validation(source_details,projectx_details)
        try:
            if projectx_details!='Null':
                self.writer.writerow([source_id,projectx_id,show_type,source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"]
                ,meta_data_validation_result["aliases_match"],meta_data_validation_result["release_year_match"],meta_data_validation_result["duration_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],meta_data_validation_result["source_link_present"]
                ,images_validation_result[0],images_validation_result[1],credits_validation_result[0],credits_validation_result[1],only_mapped_ids[2]])
            else:
                self.writer.writerow([source_id,projectx_id,show_type,'','','','','',''
                ,'','','','','','','','','','','','','Px_response_null'])    
        except Exception as e:
            print (type(e))
            pass                

    #TODO: Getting_projectx_ids which is only mapped to Vudu
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/vudu_meta_data_checking%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            fieldnames = ["%s_id"%self.source,"Projectx_id","show_type","%s_title"%self.source,"Px_title"
                        ,"Px_episode_title","title_match","description_match","genres_match","aliases_match",
                        "release_year_match","duration_match","season_number_match","episode_number_match",
                        "px_video_link_present","%s_link_present"%self.source,"image_url_missing","Wrong_url",
                        "credit_match","credit_mismatch"]
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(fieldnames)
            projectx_id=0   
            source_id=0
            #import pdb;pdb.set_trace()
            for aa in range(start_id,end_id,1000):
                try:
                    query_vudu=self.sourcetable.aggregate([{"$match":{"$and":[{"show_type":{"$in":["MO","SE","SM"]}},{"language" : "English"}]}},{"$project":{"launch_id":1,"_id":0,"show_type":1,"series_id":1,"title":1,"release_year":1,"episode_number":1,"season_number":1,"alternate_title":1,"duration":1,"image":1,"url":1,"description":1,"credit_summary":1,"category":1}},{"$skip":aa},{"$limit":1000}])
                except (Exception,socket.error) as e:
                    query_vudu=self.sourcetable.aggregate([{"$match":{"$and":[{"show_type":{"$in":["MO","SE","SM"]}},{"language" : "English"}]}},{"$project":{"launch_id":1,"_id":0,"show_type":1,"series_id":1,"title":1,"release_year":1,"episode_number":1,"season_number":1,"alternate_title":1,"duration":1,"image":1,"url":1,"description":1,"credit_summary":1,"category":1}},{"$skip":aa},{"$limit":1000}])    
                for data in query_vudu:
                    if data.get("launch_id")!="":
                        #import pdb;pdb.set_trace()
                        vudu_id=data.get("launch_id")
                        show_type=data.get("show_type")
                        self.count_vudu_id+=1
                        print("\n")
                        print("%s_id:"%self.source,vudu_id,"thread_name:",thread_name,"count_vudu_id:"+str(self.count_vudu_id),
                                                                 "name:"+str(thread_name))
                        only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id(vudu_id,show_type,self.source,self.px_mappingdb_cur)
                        if only_mapped_ids[2]=='True':
                            self.total+=1
                            #import pdb;pdb.set_trace()
                            projectx_id=only_mapped_ids[0]
                            source_id=only_mapped_ids[1]
                            print("\n")
                            print ({"total":self.total,"id":vudu_id,"Px_id":projectx_id,
                                "%s_id"%self.source:source_id,"thread_name":thread_name,"source_map":only_mapped_ids[2]})
                            self.meta_data_validation_(data,projectx_id,source_id,show_type,thread_name,only_mapped_ids)
        output_file.close()
        self.connection.close()                    
        self.connection_pxmapping.close()
        self.px_mappingdb_cur.close()


    #TODO: to set up threading part
    def threading_pool(self):    

        t1=Process(target=self.main,args=(1,"thread-1",20000))
        t1.start()
        t2=Process(target=self.main,args=(20000,"thread-2",40000))
        t2.start()
        t3=Process(target=self.main,args=(40000,"thread-3",60000))
        t3.start()
        t4=Process(target=self.main,args=(60000,"thread-4",80000))
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
        t11=Process(target=self.main,args=(200000,"thread-11",240000))
        t11.start()

    #starting     
mata_data_validation_vudu().threading_pool()


#220570
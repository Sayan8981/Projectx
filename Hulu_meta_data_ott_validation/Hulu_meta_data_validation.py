"""Writer: Saayan"""

from multiprocessing import Process
import csv
import threading
import datetime
import sys
import urllib2
import json
import pymongo
import MySQLdb
import os
from urllib2 import URLError,HTTPError
import httplib
import socket
import pinyin
import unidecode
import time
from time import gmtime, strftime
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules,ott_meta_data_validation_modules
sys.setrecursionlimit(1500)


class meta_data:
    retry_count=0
    def init(self):
        self.hulu_credit_present='Null'
        self.hulu_genres=[]
        self.hulu_duration=''
        self.hulu_title=''
        self.hulu_description=''
        self.hulu_credits=[]
        self.hulu_credit_present=''
        self.hulu_alternate_titles=[]
        self.hulu_release_year=0
        self.hulu_link_present='Null'
        self.hulu_episode_number=0
        self.hulu_season_number=0

    def getting_images(self,source_id,show_type,source_show_id,details):
        #import pdb;pdb.set_trace()
        images_details=[]

        if details.get("thumbnails"):
            for image in details.get("thumbnails"):
                images_details.append({'url':image.get("url")})
        if show_type=='SM':
            images_details.append({'url':details.get("series").get("series_art")})
                
        return images_details    

    #TODO: to get source meta details of source_id
    def getting_source_details(self,hulu_id,show_type,source,thread_name,details):
        #import pdb;pdb.set_trace()
        self.init()
        try:
            if show_type!="SM":
                try:        
                    self.hulu_title=unidecode.unidecode(pinyin.get(details.get("title")))
                except TypeError:
                    return self.hulu_title
                try:        
                    self.hulu_description=unidecode.unidecode(pinyin.get(details.get("description")))
                except TypeError:
                    return self.hulu_description
                try:
                    self.hulu_release_year=details.get("original_premiere_date").split("-")[0]
                except Exception:
                    return self.hulu_release_year             
            else:        
                try:        
                    self.hulu_title=unidecode.unidecode(pinyin.get(details.get("series").get("name")))
                except TypeError:
                    return self.hulu_title
                try:        
                    self.hulu_description=unidecode.unidecode(pinyin.get(details.get("series").get("description")))
                except TypeError:
                    return self.hulu_description
                try:
                    self.hulu_release_year=details.get("series").get("original_premiere_date").split("-")[0]
                except Exception:
                    return hulu_release_year           


            self.hulu_show_id=details.get("series").get("site_id")

            self.hulu_link=details.get("link")
            if self.hulu_link!="" or self.hulu_link is not None :
                self.hulu_link_present='True' 
            #import pdb;pdb.set_trace()
            if show_type=='SE':
                self.hulu_episode_number=details.get("number")
                self.hulu_season_number=details.get("season").get("number")

            self.hulu_images_details=self.getting_images(hulu_id,show_type,self.hulu_show_id,details)

            return {"source_credits":self.hulu_credits,"source_credit_present":self.hulu_credit_present,"source_title":self.hulu_title,"source_description":self.hulu_description,
                    "source_genres":self.hulu_genres,"source_alternate_titles":self.hulu_alternate_titles,"source_release_year":self.hulu_release_year
                   ,"source_duration":self.hulu_duration,"source_season_number":self.hulu_season_number,"source_episode_number":self.hulu_episode_number,
                   "source_link_present":self.hulu_link_present,"source_images_details":self.hulu_images_details}
            
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError,pymongo.errors.CursorNotFound) as e:
            self.retry_count<=5
            print ("exception caught getting_source_details...............",type(e),hulu_id,show_type,source,thread_name)
            print ("\n") 
            print ("Retrying.............",self.retry_count)
            if self.retry_count<=5:
                self.getting_source_details(hulu_id,show_type,source,name,details)    
            else:
                self.retry_count=0    


class hulu_meta_data_validation:
    retry_count=0
    def __init__(self):
        self.source="Hulu"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.expired_token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        self.total_mo=0
        self.total_se=0
        self.total_sm=0
        self.count_valid_programs=0
        self.Not_valid_programs_count=0
        self.count_hulu_id_se=0
        self.count_hulu_id_mo=0
        self.writer=''
        self.link_expired=''

    def mongo_mysql_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
        self.sourceDB=self.connection["qadb"] 
        self.sourcetable_movies=self.sourceDB["HuluValidMovies"]
        self.sourcetable_episodes=self.sourceDB["HuluValidEpisodes"]


    def get_env_url(self):
        self.prod_domain="api.caavo.com"
        self.expired_api='https://%s/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=%s'
        self.source_mapping_api="http://54.175.96.97:81/projectx/mappingfromsource?sourceIds=%d&sourceName=%s&showType=%s"
        self.projectx_programs_api='https://projectx.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api='http://54.175.96.97:81/projectx/%d/mapping/'
        self.projectx_duplicate_api='http://54.175.96.97:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'


    #TODO: to validate the meta data population and printing to o/p sheet
    def validation_result(self,source_id,show_type,data,projectx_id,thread_name,object_):
        #import pdb;pdb.set_trace()
        object_.init()
        source_details=object_.getting_source_details(source_id,show_type,self.source,thread_name,data)
        projectx_details=ott_meta_data_validation_modules().getting_projectx_details(projectx_id,show_type,self.source,thread_name,
                          self.projectx_programs_api,self.token)
        if projectx_details!='Null':
            #TODO: creating object for meta data validation module class
            meta_validation_object_=ott_meta_data_validation_modules().meta_data_validate_hulu()
            meta_validation_object_.cleanup()
            meta_data_validation_result=meta_validation_object_.meta_data_validation(source_id,source_details,projectx_details,show_type)

            if show_type=='MO' or show_type=='SE':
                self.link_expired=lib_common_modules().link_expiry_check_(self.expired_api,self.prod_domain,source_id,self.source,self.expired_token)
                ott_validation_result=ott_meta_data_validation_modules().ott_validation(projectx_details,data.get("id"))
            images_validation_result=ott_meta_data_validation_modules().images_validation(source_details,projectx_details)
            try:
                self.writer.writerow([source_id,data.get("id"),projectx_id,show_type,
                            source_details["source_title"],projectx_details["px_long_title"],
                            projectx_details["px_episode_title"],meta_data_validation_result["title_match"],
                            meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"],
                            meta_data_validation_result["release_year_match"],meta_data_validation_result["season_number_match"],
                            meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],
                            meta_data_validation_result["source_link_present"],images_validation_result[0],images_validation_result[1],
                            ott_validation_result,'',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),
                            "Local: "+strftime("%Y-%m-%d %d:%M:%S %p"),'','',self.link_expired])
            except Exception:
                #import pdb;pdb.set_trace()
                self.writer.writerow([source_id,data.get("id"),projectx_id,show_type,source_details["source_title"]
                           ,projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"]
                           ,meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"],meta_data_validation_result["release_year_match"]
                           ,meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"]
                           ,meta_data_validation_result["source_link_present"],images_validation_result[0],images_validation_result[1]])
        else:
            self.writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','',
                            '','','','','','','','','','','','','','','','Px_response_null',''
                            ,"GMT: "+time.strftime("%Y-%m-%d %d:%M:%S %p", time.gmtime()),
                            "Local: "+strftime("%Y-%m-%d %d:%M:%S %p")])        

    # TODO: going for meta data validation for movies
    def meta_data_validation_mo(self,data,projectx_id,source_id,thread_name):
        #import pdb;pdb.set_trace()
        object_mo=meta_data()
        object_mo.init()
        self.total_mo+=1
        try:
            hulu_id=data.get("site_id")
            show_type=data.get("programming_type")
            if show_type=="Full Movie":
                show_type='MO'        
            only_mapped_ids=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api_hulu(
                                           str(hulu_id),self.source_mapping_api,self.projectx_mapping_api
                                                             ,show_type,self.source,self.token)
            #if only_mapped_ids is not None:
            if only_mapped_ids["source_flag"]=='True':
                #import pdb;pdb.set_trace()
                projectx_id=only_mapped_ids["px_id"]
                source_id=only_mapped_ids["source_id"]
                print("\n")
                print({"total_mo":self.total_mo,"source_mo_id":hulu_id,"thread_name":thread_name
                                ,"Px_id":projectx_id,"%s_id"%self.source:source_id})
                self.validation_result(source_id,show_type,data,projectx_id,thread_name,object_mo)
            elif only_mapped_ids["source_flag"]=='True(Rovi+others)':
                #TODO: to check only OTT link population for the episodes
                #import pdb;pdb.set_trace()
                projectx_id=only_mapped_ids["px_id"]
                source_id=hulu_id
                print("\n")
                print ({"total_mo":self.total_mo,"Px_id":projectx_id,"%s_id"%self.source:source_id,
                                                                     "thread_name":thread_name})
                projectx_details=ott_meta_data_validation_modules().getting_projectx_details(projectx_id,show_type,self.source,thread_name,
                                                  self.projectx_programs_api,self.token)
                if projectx_details!='Null':
                    if data.get("id") is not None:
                        self.link_expired=lib_common_modules().link_expiry_check_(self.expired_api,self.prod_domain,hulu_id,self.source,self.expired_token)
                        ott_validation_result=ott_meta_data_validation_modules().ott_validation(projectx_details,data.get("id"))
                        try:
                            self.writer.writerow([source_id,data.get("id"),projectx_id,show_type,
                                             '','','','','','','','','','','','','',
                                             ott_validation_result,only_mapped_ids["source_flag"],'','',
                                             "GMT: "+time.strftime("%Y-%m-%d %d:%M:%S %p", time.gmtime()),
                                             "Local: "+strftime("%Y-%m-%d %d:%M:%S %p"),self.link_expired])
                        except Exception as e:
                            print ("Exception caught ....................",type(e),hulu_id,show_type)
                            pass
                else:
                    self.writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','','',''
                                        ,'','','','','','','','','','',only_mapped_ids["source_flag"],'',
                                        'Px_response_null','',"GMT: "+time.strftime("%Y-%m-%d %d:%M:%S %p", time.gmtime()),
                                        "Local: "+strftime("%Y-%m-%d %d:%M:%S %p")])    
            else:
                self.writer.writerow([hulu_id,'','',show_type,'','','','','','','','','',''
                                    ,'','','','','','','','',only_mapped_ids,'','Px_id_null','',
                                    "GMT: "+time.strftime("%Y-%m-%d %d:%M:%S %p", time.gmtime()),
                                    "Local: "+strftime("%Y-%m-%d %d:%M:%S %p")])
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
            self.retry_count+=1
            print("\n")
            print("Retrying...................................",self.retry_count)
            print("\n")
            print ("exception/error caught in (validation_mo).................",type(e),hulu_id,show_type,thread_name)
            if self.retry_count<=5:
                self.meta_data_validation_mo(data,projectx_id,source_id,thread_name)        
            else:
                self.retry_count=0    

    # TODO: going for meta data validation for episodes and series
    def meta_data_validation_se_sm(self,data,projectx_id,source_id,array_sm,thread_name):
        #import pdb;pdb.set_trace()
        object_se=meta_data()
        try:
            if data.get("site_id") is not None:
                self.total_se+=1
                #import pdb;pdb.set_trace()
                hulu_id_se=data.get("site_id")
                show_type=data.get("programming_type")
                if show_type=="Full Episode":
                    show_type='SE'
                only_mapped_ids_se=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api_hulu(
                                       str(hulu_id_se),self.source_mapping_api,self.projectx_mapping_api
                                                ,show_type,self.source,self.token)
                #if only_mapped_ids_se is not None:
                if only_mapped_ids_se["source_flag"]=='True':
                    #import pdb;pdb.set_trace()
                    projectx_id=only_mapped_ids_se["px_id"]
                    source_id=only_mapped_ids_se["source_id"]
                    print("\n")
                    print({"total_se":self.total_se,"_hulu_se_id":hulu_id_se,"thread_name":thread_name
                                             ,"Px_id":projectx_id,"%s_id"%self.source:source_id})
                    self.validation_result(source_id,show_type,data,projectx_id,thread_name,object_se)
                elif only_mapped_ids_se["source_flag"]=='True(Rovi+others)':
                    #TODO: to check only OTT link population for the movies
                    #import pdb;pdb.set_trace()
                    projectx_id=only_mapped_ids_se["px_id"]
                    source_id=only_mapped_ids_se["source_id"]
                    print("\n")
                    print ({"Px_id":projectx_id,"%s_id"%self.source:source_id,"thread_name":thread_name})
                    projectx_details=ott_meta_data_validation_modules().getting_projectx_details(projectx_id,show_type,self.source,
                                           thread_name,self.projectx_programs_api,self.token)
                    if projectx_details!='Null':
                        if data.get("id") is not None:
                            self.link_expired=lib_common_modules().link_expiry_check_(self.expired_api,self.prod_domain,hulu_id_se,self.source,self.expired_token)
                            ott_validation_result=ott_meta_data_validation_modules().ott_validation(projectx_details,data.get("id"))
                            try:
                                self.writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','','',
                                                       '','','','','','','','','',ott_validation_result,only_mapped_ids_se["source_flag"],
                                                       '','',"GMT: "+time.strftime("%Y-%m-%d %d:%M:%S %p",
                                                        time.gmtime()),"Local: "+strftime("%Y-%m-%d %d:%M:%S %p"),self.link_expired])
                            except Exception as e:
                                print ("Exception caught ....................",type(e),hulu_id_se,show_type)
                                pass    
                    else:
                        self.writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','','',''
                                     ,'','','','','','','','','','','',only_mapped_ids_se["source_flag"],''
                                     ,'Px_response_null','',"GMT: "+time.strftime("%Y-%m-%d %d:%M:%S %p", time.gmtime()),
                                     "Local: "+strftime("%Y-%m-%d %d:%M:%S %p")])
                else:
                    self.writer.writerow([hulu_id_se,'','',show_type,'','','','','','','','','','','',
                              '','','','','','','',only_mapped_ids_se,'','Px_id_null','',
                              "GMT: "+time.strftime("%Y-%m-%d %d:%M:%S %p", time.gmtime()),
                              "Local: "+strftime("%Y-%m-%d %d:%M:%S %p")])        
                
            #TODO: for series
            if data.get("series").get("site_id") is not None:
                #import pdb;pdb.set_trace()
                hulu_id_sm=data.get("series").get("site_id")
                if hulu_id_sm not in array_sm:
                    array_sm.append(hulu_id_sm)
                    show_type='SM'
                    only_mapped_ids_sm=ott_meta_data_validation_modules().getting_mapped_px_id_mapping_api_hulu(
                                          str(hulu_id_sm),self.source_mapping_api,self.projectx_mapping_api
                                                         ,show_type,self.source,self.token)
                    if only_mapped_ids_sm is not None:
                        if only_mapped_ids_sm["source_flag"]=='True':
                            self.total_sm+=1
                            #import pdb;pdb.set_trace()
                            projectx_id=only_mapped_ids_sm["px_id"]
                            source_id=only_mapped_ids_sm["source_id"]
                            print("\n")
                            print({"total_sm":self.total_sm,"source_sm_id":hulu_id_sm,"thread_name"
                                        :thread_name,"Px_id":projectx_id,"%s_id"%self.source:source_id})
                            self.validation_result(source_id,show_type,data,projectx_id,thread_name,object_se)        
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError,pymongo.errors.CursorNotFound) as e:
            self.retry_count+=1
            print ("Exception caught (meta_data_validation_se_sm)....................",type(e),data.get("site_id"),thread_name)
            print ("\n")
            print ("retrying..................",self.retry_count)
            if self.retry_count<=5:
                self.meta_data_validation_se_sm(data,projectx_id,source_id,array_sm,thread_name)
            else:
                self.retry_count=0                        

    #TODO: to open file for writing o/p
    def main(self,start_id,thread_name,end_id,page_id):
        #import pdb;pdb.set_trace()
        self.get_env_url()
        self.mongo_mysql_connection()
        result_sheet='/result/hulu_meta_data_checking_%s_%s.csv'%(page_id,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            fieldnames_tag = ["%s_id"%self.source,"launch_id","Projectx_id","show_type","%s_title"%self.source,"Px_title","Px_episode_title","title_match","description_match",
                         "genres_match","release_year_match","season_number_match","episode_number_match","px_video_link_present","%s_link_present"%self.source,
                             "image_url_missing","Wrong_url","ott_link_result",'','','','','',"Link_Expired"]
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(fieldnames_tag)
            projectx_id=0
            source_id=0
            #import pdb;pdb.set_trace()
            if end_id>2000:
                array_sm=[]
                for aa in range(start_id,end_id,100):
                    try:
                        query_hulu=self.sourcetable_episodes.aggregate([{"$match":{"programming_type" : "Full Episode","video_language" : "en"}},{"$project":{"site_id":1,
                                    "_id":0,"programming_type":1,"series.site_id":1,"title":1,"series.name":1,"series.original_premiere_date":1,"original_premiere_date":1,
                                    "duration":1,"thumbnails":1,"url":1,"series.description":1,"description":1,"number":1,"season.number":1,"link":1,
                                    "id":1,"series.series_art":1,"availability.svod.end":1}},{"$skip":aa},{"$limit":100}])
                        for data in query_hulu:
                            if data.get("availability").get("svod").get("end") is None or data.get("availability").get("svod").get("end") > datetime.datetime.now().isoformat():   
                                #import pdb;pdb.set_trace()
                                print("\n")
                                print(datetime.datetime.now())
                                print("\n")
                                self.count_hulu_id_se+=1    
                                print("count_hulu_id_se:",self.count_hulu_id_se,"name:",thread_name)
                                self.meta_data_validation_se_sm(data,projectx_id,source_id,array_sm,thread_name)
                            else:
                                self.Not_valid_programs_count+=1
                                print({"Not_valid_programs_count":self.Not_valid_programs_count,"thread":thread_name,"id":data.get("site_id")})
                    except (Exception,pymongo.errors.OperationFailure,pymongo.errors.ServerSelectionTimeoutError,pymongo.errors.CursorNotFound,RuntimeError) as e:
                        print ("Exception in query...........",type(e))
                        pass
            else:            
                for aa in range(start_id,end_id,10):
                    try:
                        query_hulu=self.sourcetable_movies.aggregate([{"$match":{"programming_type" : "Full Movie"}},{"$project":{"site_id":1,"_id":0,
                                         "programming_type":1,"series.site_id":1,"title":1,"series.original_premiere_date":1,
                                         "duration":1,"thumbnails":1,"url":1,"series.description":1,"link":1,"id":1,"original_premiere_date":1,
                                         "description":1,"availability.svod.end":1}},{"$skip":aa},{"$limit":10}])
                        for data in query_hulu:
                            if data.get("availability").get("svod").get("end") is None or data.get("availability").get("svod").get("end") > datetime.datetime.now().isoformat():
                                if data.get("site_id")!="":
                                    #import pdb;pdb.set_trace()
                                    self.count_hulu_id_mo+=1
                                    print("\n")
                                    print(datetime.datetime.now())
                                    print("\n")     
                                    print("count_hulu_id:",self.count_hulu_id_mo,"name:",thread_name)
                                    self.meta_data_validation_mo(data,projectx_id,source_id,thread_name)
                    except (Exception,pymongo.errors.OperationFailure,pymongo.errors.ServerSelectionTimeoutError,pymongo.errors.CursorNotFound,RuntimeError) as e:
                        print ("Exception in query...........",type(e))
                        pass

        self.connection.close()
        output_file.close()                            
                    
            

    #TODO : to set up threads
    def threading_pool(self):    

        """t1=Process(target=self.main,args=(0,"thread-1",2000,'MO'))
        t1.start()"""
        t2=Process(target=self.main,args=(0,"thread-2",20000,'SE1'))
        t2.start()
        t3=Process(target=self.main,args=(20000,"thread-3",40000,'SE2'))
        t3.start()
        t4=Process(target=self.main,args=(40000,"thread-4",60000,'SE3'))
        t4.start()
        t5=Process(target=self.main,args=(60000,"thread-5",80000,'SE4'))
        t5.start()
        t6=Process(target=self.main,args=(80000,"thread-6",100000,'SE5'))
        t6.start()
  

# Starting     
hulu_meta_data_validation().threading_pool()



"""Writer: Saayan"""

import sys
import urllib2
import json
import os
from urllib2 import HTTPError
import httplib
import socket
import pinyin
import pymongo
import unidecode
sys.path.insert(0,os.getcwd()+'/common_lib')
sys.path.insert(1,os.getcwd()+'/common_lib/Px_lib')
sys.path.insert(2,os.getcwd()+'/common_lib/source_lib')
sys.path.insert(3,os.getcwd()+'/common_lib/validation_lib')
import initialization_file
import getting_px_images
import getting_images_hulu


class meta_data:

    def __init__(self):
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

        self.px_long_title=''
        self.px_video_link=[]
        self.px_credit_present='False'
        self.px_video_link_present='False'
        self.px_original_title=''
        self.px_episode_title=''
        self.px_run_time=0
        self.px_release_year=0
        self.px_record_language=''
        self.px_description=''
        self.px_images_details=[]
        self.px_genres=[]
        self.px_aliases=[]
        self.launch_id=[]
        self.px_response='Null'
        self.px_credits=[]
        self.px_season_number=0
        self.px_episode_number=0

    #TODO: to get source meta details of source_id
    def getting_source_details(self,hulu_id,show_type,source,thread_name,details,db_table):
        #import pdb;pdb.set_trace()
        retry_count=0
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

            self.hulu_images_details=getting_images_hulu.getting_images(hulu_id,show_type,self.hulu_show_id,details,db_table)

            return {"source_credits":self.hulu_credits,"source_credit_present":self.hulu_credit_present,"source_title":self.hulu_title,"source_description":self.hulu_description,
                    "source_genres":self.hulu_genres,"source_alternate_titles":self.hulu_alternate_titles,"source_release_year":self.hulu_release_year
                   ,"source_duration":self.hulu_duration,"source_season_number":self.hulu_season_number,"source_episode_number":self.hulu_episode_number,
                   "source_link_present":self.hulu_link_present,"source_images_details":self.hulu_images_details}
            
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError,pymongo.errors.CursorNotFound) as e:
            retry_count<=5
            print ("exception caught ................................................................",type(e),hulu_id,show_type,source,thread_name)
            print ("\n") 
            print ("Retrying.............")
            if retry_count<=5:
                self.getting_source_details(hulu_id,show_type,source,name,details,db_table)    
            else:
                retry_count=0    

    #TODO: to get meta details of projectx ids
    def getting_projectx_details(self,projectx_id,show_type,source,thread_name):
        #import pdb;pdb.set_trace()
        retry_count=0
        try:
            projectx_api=initialization_file.projectx_preprod_domain%projectx_id
            px_resp=urllib2.urlopen(urllib2.Request(projectx_api,None,{'Authorization':initialization_file.token}))
            data_px=px_resp.read()
            data_px_resp=json.loads(data_px)
            if data_px_resp!=[]:

                for data in data_px_resp:
                    if data.get("long_title") is not None and data.get("long_title")!="":
                        self.px_long_title=unidecode.unidecode(pinyin.get(data.get("long_title")))
                    if data.get("original_title") is not None and data.get("original_title")!="":
                        self.px_original_title=unidecode.unidecode(pinyin.get(data.get("original_title")))
                    if data.get("original_episode_title")!="": 
                        self.px_episode_title=unidecode.unidecode(pinyin.get(data.get("original_episode_title")))
                    
                    self.px_record_language= data.get("record_language")
                    self.px_release_year=data.get("release_year")
                    self.px_run_time=data.get("run_time")
                    self.px_show_id=data.get("series_id")
                    try:
                        self.px_description=pinyin.get(data.get("description")[0].get("program_description"))
                    except Exception:
                        return self.px_description           
                    try:
                        self.px_season_number=data.get("episode_season_number")
                    except Exception:
                        return self.px_season_number    
                    #import pdb;pdb.set_trace()
                    try:
                        self.px_episode_number= data.get("episode_season_sequence")
                    except Exception:
                        return self.px_episode_number     

                    self.px_video_link= data.get("videos")
                    if self.px_video_link:
                        self.px_video_link_present='True'
                        for linkid in self.px_video_link:
                            #if linkid.get("source_id")=='hulu':
                            self.launch_id.append(linkid.get("launch_id"))

                    if data.get("images"):
                        self.px_images_details=getting_px_images.px_images_details(self.px_show_id,data.get("images"),show_type)

                    return {"px_credits":self.px_credits,"px_credit_present":self.px_credit_present,"px_long_title":self.px_long_title,"px_episode_title":self.px_episode_title,
                           "px_original_title":self.px_original_title,"px_description":self.px_description,"px_genres":self.px_genres,"px_aliases":self.px_aliases,
                            "px_release_year":self.px_release_year,"px_run_time":self.px_run_time,"px_season_number":self.px_season_number,"px_episode_number":self.px_episode_number,
                            "px_video_link_present":self.px_video_link_present,"px_images_details":self.px_images_details,"launch_id":self.launch_id}
            else:
                return self.px_response

        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError,pymongo.errors.CursorNotFound) as e:
            retry_count+=1
            print ("exception caught ..............................................",type(e),projectx_id,show_type,source,thread_name)
            print ("\n") 
            print ("Retrying.............")
            if retry_count<=5:
                self.getting_projectx_details(projectx_id,show_type,source,name)    
            else:
                retry_count=0    
        


         


            



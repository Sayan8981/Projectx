"""Writer: Saayan"""

import sys
import csv
import urllib2
import json
import os
from urllib2 import HTTPError
import httplib
import socket
import pinyin
import unidecode
sys.path.insert(0,os.getcwd()+'/common_lib')
sys.path.insert(1,os.getcwd()+'/common_lib/Px_lib')
sys.path.insert(2,os.getcwd()+'/common_lib/source_lib')
sys.path.insert(3,os.getcwd()+'/common_lib/validation_lib')
import initialization_file
  


def getting_source_details(hbogo_id,show_type,source,thread_name,details,id_):
    #import pdb;pdb.set_trace()
    hbogo_duration=''
    hbogo_title=''
    hbogo_description=''
    hbogo_release_year=0
    hbogo_link_present='Null'
    try:
        try:        
            hbogo_title=unidecode.unidecode(pinyin.get((details[id_])[2]))
        except TypeError:
            pass
        try:        
            hbogo_description=pinyin.get((details[id_])[5])
        except TypeError:
            pass

        hbogo_show_id=(details[id_])[4]

        hbogo_link=(details[id_])[6]
        if hbogo_link!="" or hbogo_link is not None :
            hbogo_link_present='True' 
        try:
            hbogo_release_year=eval((details[id_])[9])
        except Exception:
            pass    
        #import pdb;pdb.set_trace()
        hbogo_duration=(details[id_])[10]
        if hbogo_duration is None or hbogo_duration==0:
            hbogo_duration=0

        #import pdb;pdb.set_trace()
        hbogo_episode_number=(details[id_])[8]
        hbogo_season_number=(details[id_])[7]

        return {"source_title":hbogo_title,"source_description":hbogo_description,"source_release_year":hbogo_release_year,"source_duration":hbogo_duration,
        "source_season_number":hbogo_season_number,"source_episode_number":hbogo_episode_number,"source_link_present":hbogo_link_present}
        
    except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
        retry_count+=1
        print ("exception caught getting_source_details func..........................................",hbogo_id,show_type,source,thread_name)
        print ("\n") 
        print ("Retrying.............",retry_count)
        print ("\n")    
        if retry_count<=5:
            getting_source_details(hbogo_id,show_type,source,thread_name,details,db_table)    
        else:
            retry_count=0    



def getting_projectx_details(projectx_id,show_type,source,thread_name):

    retry_count=0
    px_long_title=''
    px_video_link=[]
    px_video_link_present='False'
    px_original_title=''
    px_episode_title=''
    px_run_time=0
    px_release_year=0
    px_record_language=''
    px_description=''
    px_response='Null'
    launch_id=[]
    px_season_number=0
    px_episode_number=0
    #import pdb;pdb.set_trace()
    try:
        projectx_api=initialization_file.projectx_preprod_domain%projectx_id
        px_resp=urllib2.urlopen(urllib2.Request(projectx_api,None,{'Authorization':initialization_file.token}))
        data_px=px_resp.read()
        data_px_resp=json.loads(data_px)
        if data_px_resp!=[]:

            for data in data_px_resp:
                if data.get("long_title") is not None and data.get("long_title")!="":
                    px_long_title=unidecode.unidecode(pinyin.get(data.get("long_title")))
                if data.get("original_title") is not None and data.get("original_title")!="":
                    px_original_title=unidecode.unidecode(pinyin.get(data.get("original_title")))
                if data.get("original_episode_title")!="": 
                    px_episode_title=unidecode.unidecode(pinyin.get(data.get("original_episode_title")))
                elif data.get("episode_title")!="":
                    px_episode_title=unidecode.unidecode(pinyin.get(data.get("episode_title")))                       
                
                px_record_language= data.get("record_language")
                px_release_year=data.get("release_year")
                px_run_time=data.get("run_time")
                px_show_id=data.get("series_id")
                try:
                    px_description=unidecode.unidecode(pinyin.get(data.get("description")[0].get("program_description")))
                except Exception:
                    return px_description    

                #import pdb;pdb.set_trace()
                try:
                    px_season_number=data.get("episode_season_number")
                except Exception:
                    return px_season_number
                try:    
                    px_episode_number=data.get("episode_season_sequence")
                except Exception:
                    return px_episode_number

                px_video_link= data.get("videos")
                if px_video_link:
                    px_video_link_present='True'
                    for linkid in px_video_link:
                        if linkid.get("source_id")=='hbogo' and linkid.get("fetched_from")=='Vudu':
                            launch_id.append(linkid.get("launch_id"))

                #(hbogo_credits,hbogo_credit_present,hbogo_title,hbogo_original_title,hbogo_description,hbogo_genres,hbogo_alternate_titles,hbogo_release_year,hbogo_duration,hbogo_first_aired,hbogo_season_number,hbogo_episode_number,comment_link)
                return {"px_long_title":px_long_title,"px_episode_title":px_episode_title,"px_original_title":px_original_title,"px_description":px_description,"px_release_year":px_release_year,
                "px_run_time":px_run_time,"px_season_number":px_season_number,"px_episode_number":px_episode_number,"px_video_link_present":px_video_link_present,
                "launch_id":launch_id}
        else:
            return px_response

    except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
        retry_count+=0
        print ("exception caught getting_projectx_details func...............................",projectx_id,show_type,source,thread_name)
        print ("\n") 
        print ("Retrying.............",retry_count)
        print ("\n")    
        if retry_count<=5:
            getting_projectx_details(projectx_id,show_type,source,thread_name)    
        else:
            retry_count=0    
        


         


            



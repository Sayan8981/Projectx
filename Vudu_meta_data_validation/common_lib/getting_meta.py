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
import getting_px_credits
import getting_px_images
import getting_aliases_px
import getting_images_vudu
import getting_mapped_source_credits
   


def getting_source_details(vudu_id,show_type,source,thread_name,details,db_table):
    #import pdb;pdb.set_trace()
    retry_count=0
    vudu_credit_present='Null'
    vudu_genres=[]
    vudu_duration=''
    vudu_title=''
    vudu_description=''
    vudu_alternate_titles=[]
    vudu_release_year=0
    vudu_link_present='Null'
    try:
        vudu_credits=getting_mapped_source_credits.getting_credits(details)
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

        vudu_images_details=getting_images_vudu.getting_images(vudu_id,show_type,vudu_show_id,details,db_table)

        return {"source_credits":vudu_credits,"source_credit_present":vudu_credit_present,"source_title":vudu_title,"source_description":vudu_description,
        "source_genres":vudu_genres,"source_alternate_titles":vudu_alternate_titles,"source_release_year":vudu_release_year,"source_duration":vudu_duration,
        "source_season_number":vudu_season_number,"source_episode_number":vudu_episode_number,"source_link_present":vudu_link_present,"source_images_details":vudu_images_details}
        
    except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
        retry_count+=1
        print ("exception caught getting_source_details func............................................................",vudu_id,show_type,source,thread_name)
        print ("\n") 
        print ("Retrying.............",retry_count)
        print ("\n")    
        if retry_count<=5:
            getting_source_details(vudu_id,show_type,source,thread_name,details,db_table)    
        else:
            retry_count=0    



def getting_projectx_details(projectx_id,show_type,source,thread_name):

    retry_count=0
    px_long_title=''
    px_video_link=[]
    px_credit_present='False'
    px_video_link_present='False'
    px_original_title=''
    px_episode_title=''
    px_run_time=0
    px_release_year=0
    px_record_language=''
    px_description=''
    px_images_details=[]
    px_response='Null'
    px_genres=[]
    px_aliases=[]
    px_credits=[]
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

                if data.get("genres"):
                    for genres in data.get("genres"):
                        px_genres.append(genres.lower())

                if data.get("aliases"):
                    px_aliases=getting_aliases_px.px_aliases(data.get("aliases"),source)       
     
                #import pdb;pdb.set_trace()
                try:
                    px_season_number=data.get("episode_season_number")
                except Exception:
                    return px_season_number
                try:    
                    px_episode_number=data.get("episode_season_sequence")
                except Exception:
                    return px_episode_number

                if data.get("credits"):
                    px_credits=getting_px_credits.getting_px_credits(data.get("credits"))
                    px_credit_present='True'

                px_video_link= data.get("videos")
                if px_video_link:
                    px_video_link_present='True'
                    for linkid in px_video_link:
                        if linkid.get("source_id")=='vudu' and linkid.get("fetched_from")=='Vudu':
                            launch_id.append(linkid.get("launch_id"))

                if data.get("images"):
                    px_images_details=getting_px_images.px_images_details(px_show_id,data.get("images"),show_type)
                #(vudu_credits,vudu_credit_present,vudu_title,vudu_original_title,vudu_description,vudu_genres,vudu_alternate_titles,vudu_release_year,vudu_duration,vudu_first_aired,vudu_season_number,vudu_episode_number,comment_link)
                return {"px_credits":px_credits,"px_credit_present":px_credit_present,"px_long_title":px_long_title,"px_episode_title":px_episode_title,
                "px_original_title":px_original_title,"px_description":px_description,"px_genres":px_genres,"px_aliases":px_aliases,"px_release_year":px_release_year,
                "px_run_time":px_run_time,"px_season_number":px_season_number,"px_episode_number":px_episode_number,"px_video_link_present":px_video_link_present,
                "px_images_details":px_images_details,"launch_id":launch_id}
        else:
            return px_response

    except (httplib.BadStatusLine,Exception,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError):
        retry_count+=0
        print ("exception caught getting_projectx_details func..............................................",projectx_id,show_type,source,thread_name)
        print ("\n") 
        print ("Retrying.............",retry_count)
        print ("\n")    
        if retry_count<=5:
            getting_projectx_details(projectx_id,show_type,source,thread_name)    
        else:
            retry_count=0    
        


         


            



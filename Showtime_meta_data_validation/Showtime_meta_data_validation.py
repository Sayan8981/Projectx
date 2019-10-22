"""Writer: Saayan"""

import threading
import csv
import pymysql
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
sys.path.insert(0,os.getcwd()+'/common_lib')
sys.path.insert(1,os.getcwd()+'/common_lib/Px_lib')
sys.path.insert(2,os.getcwd()+'/common_lib/source_lib')
sys.path.insert(3,os.getcwd()+'/common_lib/validation_lib')
import initialization_file
import checking_only_mapped_px_id
import getting_meta
import meta_data_validation
import credits_validation
import images_validation 


"""
meta_data_validation_result={"title_match":title_match,"description_match":description_match,"release_year_match":release_year_match,
            "duration_match":duration_match,"season_number_match":season_number_match,"episode_number_match":episode_number_match,"px_video_link_present":px_video_link_present,"source_link_present":source_link_present} 
projectx_details={"px_credits":px_credits,"px_credit_present":px_credit_present,"px_long_title":px_long_title,"px_episode_title":px_episode_title,
                "px_original_title":px_original_title,"px_description":px_description,"px_release_year":px_release_year,
                "px_run_time":px_run_time,"px_season_number":px_season_number,"px_episode_number":px_episode_number,"px_video_link_present":px_video_link_present,
                "px_images_details":px_images_details,"launch_id":launch_id}

source_details={"source_credits":showtime_credits,"source_credit_present":showtime_credit_present,"source_title":showtime_title,"source_description":showtime_description,
        "source_release_year":showtime_release_year,"source_duration":showtime_duration,
        "source_season_number":showtime_season_number,"source_episode_number":showtime_episode_number,"source_link_present":showtime_link_present,"source_images_details":showtime_images_details}                            
"""

#TODO: Validation
def meta_data_validation_(showtime_data,id_,projectx_id,source_id,show_type,source,thread_name,writer,only_mapped_ids):
    #import pdb;pdb.set_trace()
    source_details=getting_meta.getting_source_details(source_id,show_type,source,thread_name,showtime_data,id_)
    projectx_details=getting_meta.getting_projectx_details(projectx_id,show_type,source,thread_name)

    meta_data_validation_result=meta_data_validation.meta_data_validation(source_id,source_details,projectx_details,show_type)

    credits_validation_result=credits_validation.credits_validation(source_details,projectx_details)

    images_validation_result=images_validation.images_validation(source_details,projectx_details)
    
    #title_match,description_match,genres_match,aliases_match,release_year_match,duration_match,season_number_match,episode_number_match,px_video_link_present,source_link_present
    try:
        writer.writerow([source_id,projectx_id,show_type,source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"]
        ,meta_data_validation_result["release_year_match"],meta_data_validation_result["duration_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],meta_data_validation_result["source_link_present"]
        ,images_validation_result[0],images_validation_result[1],credits_validation_result[0],credits_validation_result[1],only_mapped_ids[2]])
    except Exception as e:
        print (type(e))
        pass 

def mysql_connection_query():
    retry_count=0
    connection=pymysql.connect(user="root",passwd="branch@123",host="192.168.86.10",db="branch_service",port=3306)
    cur=connection.cursor()
    try:
        query="select source_program_id,item_type,title,series_id,description,url,img_url,directors,cast,season_number,episode_number,year,run_time,expired,expiry_date,updated_at from showtime_programs;"
        cur.execute(query)
        showtime_data=cur.fetchall()
        return showtime_data
    except (Exception,socket.error) as e:
        retry_count+=1
        if retry_count<=5:
            mysql_connection_query()        
    connection.close()

#TODO: output file opening for writing O/P
def create_csv(result_sheet):
    #import pdb;pdb.set_trace()
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    output_file=open(os.getcwd()+result_sheet,"wa")
    return output_file               

#TODO: Getting_projectx_ids which is only mapped to Vudu
def main(start_id,thread_name,end_id,page_id,source):
    result_sheet='/result/showtime_meta_data_checking%d.csv'%page_id
    output_file=create_csv(result_sheet)
    with output_file as mycsvfile:
        fieldnames = ["%s_id"%source,"Projectx_id","show_type","%s_title"%source,"Px_title","Px_episode_title","title_match","description_match","release_year_match","duration_match","season_number_match","episode_number_match","px_video_link_present","%s_link_present"%source,"image_url_missing","Wrong_url","credit_match","credit_mismatch"]
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        projectx_id=0   
        source_id=0
        showtime_data=mysql_connection_query()
        #import pdb;pdb.set_trace()
        for id_ in range(start_id,end_id):
            if (showtime_data[id_])[1] is not None:
                #import pdb;pdb.set_trace()
                showtime_id=(showtime_data[id_])[0]
                show_type=(showtime_data[id_])[1]
                if show_type=='movie':
                    show_type='MO'
                elif show_type=='episode':
                    show_type='SE'
                else:
                    show_type='tv_show'       
                initialization_file.count_showtime_id+=1
                print("\n")
                print("%s_id:"%source,showtime_id,"thread_name:",thread_name,"count_showtime_id:"+str(initialization_file.count_showtime_id),
                                                                                                   "name:"+str(thread_name))
                only_mapped_ids=checking_only_mapped_px_id.getting_mapped_px_id(showtime_id,show_type,source)
                if only_mapped_ids:
                    initialization_file.total+=1
                    #import pdb;pdb.set_trace()
                    projectx_id=only_mapped_ids[0]
                    source_id=only_mapped_ids[1]
                    print("\n")
                    print ({"total":initialization_file.total,"id":showtime_id,"Px_id":projectx_id,
                        "%s_id"%source:source_id,"thread_name":thread_name,"source_map":only_mapped_ids[2]})
                    meta_data_validation_(showtime_data,id_,projectx_id,source_id,show_type,source,thread_name,writer,only_mapped_ids)
    output_file.close()                    

#TODO: to set up threading part
def threading_pool():    

    t1=threading.Thread(target=main,args=(0,"thread - 1",1000,1,initialization_file.source))
    t1.start()
    t2=threading.Thread(target=main,args=(1000,"thread - 2",2000,2,initialization_file.source))
    t2.start()
    t3=threading.Thread(target=main,args=(2000,"thread - 3",3736,3,initialization_file.source))
    t3.start()


#starting     
threading_pool()


#220570
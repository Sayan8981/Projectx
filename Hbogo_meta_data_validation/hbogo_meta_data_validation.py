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
sys.path.insert(3,os.getcwd()+'/common_lib/validation_lib')
import initialization_file
import checking_only_mapped_px_id
import getting_meta
import meta_data_validation

"""
meta_data_validation_result={"title_match":title_match,"description_match":description_match,"release_year_match":release_year_match,
            "duration_match":duration_match,"season_number_match":season_number_match,"episode_number_match":episode_number_match,"px_video_link_present":px_video_link_present,"source_link_present":source_link_present} 
projectx_details={"px_long_title":px_long_title,"px_episode_title":px_episode_title,
                "px_original_title":px_original_title,"px_description":px_description,"px_release_year":px_release_year,
                "px_run_time":px_run_time,"px_season_number":px_season_number,"px_episode_number":px_episode_number,"px_video_link_present":px_video_link_present,"launch_id":launch_id}

source_details={"source_title":hbogo_title,"source_description":hbogo_description,
        "source_release_year":hbogo_release_year,"source_duration":hbogo_duration,
        "source_season_number":hbogo_season_number,"source_episode_number":hbogo_episode_number,"source_link_present":hbogo_link_present}                            
"""

#TODO: Validation
def meta_data_validation_(hbogo_data,aa,projectx_id,source_id,show_type,source,thread_name,writer,only_mapped_ids):
    #import pdb;pdb.set_trace()
    source_details=getting_meta.getting_source_details(source_id,show_type,source,thread_name,hbogo_data,aa)
    projectx_details=getting_meta.getting_projectx_details(projectx_id,show_type,source,thread_name)

    meta_data_validation_result=meta_data_validation.meta_data_validation(source_id,source_details,projectx_details,show_type)
    try:
        writer.writerow([source_id,projectx_id,show_type,source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"]
        ,meta_data_validation_result["release_year_match"],meta_data_validation_result["duration_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],
        meta_data_validation_result["source_link_present"]])
    except Exception as e:
        print (type(e))
        pass 

def mysql_connection_query():
    retry_count=0
    connection=pymysql.connect(user="root",passwd="branch@123",host="192.168.86.10",db="branch_service",port=3306)
    cur=connection.cursor()
    try:
        query="select launch_id,show_type,title,series_title,series_launch_id,description,url,season_number,episode_number,release_year,duration,expired,expired_at,updated_at from hbogo_programs;"
        cur.execute(query)
        hbogo_data=cur.fetchall()
        return hbogo_data
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
    #import pdb;pdb.set_trace()
    result_sheet='/result/hbogo_meta_data_checking%d.csv'%page_id
    output_file=create_csv(result_sheet)
    with output_file as mycsvfile:
        fieldnames = ["%s_id"%source,"Projectx_id","show_type","%s_title"%source,"Px_title","Px_episode_title","title_match","description_match","release_year_match","duration_match","season_number_match","episode_number_match","px_video_link_present","%s_link_present"%source,]
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        projectx_id=0   
        source_id=0
        hbogo_data=mysql_connection_query()
        #import pdb;pdb.set_trace()
        for aa in range(start_id,end_id):    
            if (hbogo_data[aa])[1] is not None:
                #import pdb;pdb.set_trace()
                hbogo_id=(hbogo_data[aa])[0]
                show_type=(hbogo_data[aa])[1]
                initialization_file.count_hbogo_id+=1
                print("\n")
                print("%s_id:"%source,hbogo_id,"thread_name:",thread_name,"count_hbogo_id:"+str(initialization_file.count_hbogo_id),
                                                                                                   "name:"+str(thread_name))
                only_mapped_ids=checking_only_mapped_px_id.getting_mapped_px_id(hbogo_id,show_type,source)
                if only_mapped_ids:
                    initialization_file.total+=1
                    #import pdb;pdb.set_trace()
                    projectx_id=only_mapped_ids[0]
                    source_id=only_mapped_ids[1]
                    print("\n")
                    print ({"total":initialization_file.total,"id":hbogo_id,"Px_id":projectx_id,
                        "%s_id"%source:source_id,"thread_name":thread_name,"source_map":only_mapped_ids[2]})
                    meta_data_validation_(hbogo_data,aa,projectx_id,source_id,show_type,source,thread_name,writer,only_mapped_ids)
    output_file.close()                   

#TODO: to set up threading part
def threading_pool():    

    t1=threading.Thread(target=main,args=(1,"thread-1",2000,1,initialization_file.source))
    t1.start()
    t2=threading.Thread(target=main,args=(2000,"thread-2",4000,2,initialization_file.source))
    t2.start()
    t3=threading.Thread(target=main,args=(4000,"thread-3",6000,3,initialization_file.source))
    t3.start()
    t4=threading.Thread(target=main,args=(6000,"thread-4",6884,4,initialization_file.source))
    t4.start()

#starting     
threading_pool()


#220570
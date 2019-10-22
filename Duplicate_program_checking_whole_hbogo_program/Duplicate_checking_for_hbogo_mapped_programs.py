""" Here we are checking duplicate programs in search api """
"""Saayan"""

import threading
import pymysql
import sys
import os
import csv
from urllib2 import URLError,HTTPError
import socket
import datetime
import urllib2
import json
import httplib
import unidecode
sys.setrecursionlimit(1500)
import pinyin
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file
import Duplicate_checking_Movies
import Duplicate_checking_Series
import checking_mata_data

def duplicate_checking(hbogo_data,id_,source,writer,writer_credit_match_false,show_type,object_,object_duplicate):
    print("\n") 
    #import pdb;pdb.set_trace()
    if show_type=='MO':
        object_.cleanup()
        object_.duplicate_checking_movies(hbogo_data,id_,source,writer,writer_credit_match_false,object_duplicate)                                      
    if show_type=='SE':
        object_.cleanup()
        object_.duplicate_checking_series(hbogo_data,id_,source,writer,writer_credit_match_false,object_duplicate)
    
    print datetime.datetime.now()

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

#TODO: to create file for writing O/P
def create_csv(result_sheet):
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    return open(os.getcwd()+result_sheet,"wa")                 
           
#TODO: to get source id of vudu                      
def main(start_id,thread_name,end_id,page_id):
    #import pdb;pdb.set_trace()
    object_movies=Duplicate_checking_Movies.ProjectXMovies()
    object_series=Duplicate_checking_Series.ProjectXSeries()        
    object_duplicate=checking_mata_data.validation()
    fieldnames_tag = ["source","sm_id","id","show_type","movie_title","series_title","episode_title","release_year","link_expired","Projectx_ids","Variant id present for episode","Variant parent id for Episode","Projectx_series_id","comment","Result","Gb_id duplicate",'Duplicate ids in search',"Credit_match","Rovi_id count for duplicate","Gb_id count for duplicate","%s_id count for duplicate"%initialization_file.source,"Mapped rovi_id for duplicate","Mapped Gb_id for duplicate","Mapped %s_id for duplicate"%initialization_file.source,"variant_parent_id_present","variant_parent_id","link_fetched_from","Sources of link"]
    fieldnames_credit_match_false = ["px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary","px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary","px_id2_record_language","px_id1_credits_null","px_id1_db_credit_present","px_id2_credits_null","px_id2_db_credit_present","long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","Comment"]
    result_sheet_credit_match_false='/output1/duplicate_programs_credit_match_false_result%d.csv'%page_id
    output_file_credit_match_false=create_csv(result_sheet_credit_match_false)
    result_sheet='/output/duplicate_checked_in_search_api%d.csv'%page_id
    output_file=create_csv(result_sheet)
    with output_file as mycsvfile, output_file_credit_match_false as mycsvfile1:
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames_tag)
        writer_credit_match_false = csv.writer(mycsvfile1,dialect="excel",lineterminator = '\n')
        writer_credit_match_false.writerow(fieldnames_credit_match_false) 
        mo_list=[]
        sm_list=[]
        hbogo_data=mysql_connection_query()
        for id_ in range(start_id,end_id):
            try:
                print ("\n")
                print({"start": start_id,"end":end_id})
                if (hbogo_data[id_])[1] is not None:
                    series_title=''
                    id=(hbogo_data[id_])[0]
                    sm_id=(hbogo_data[id_])[4]
                    show_type=(hbogo_data[id_])[1].encode('utf-8')
                    if id is not None:
                        if show_type=='SE':
                            if sm_id not in sm_list:
                                sm_list.append(sm_id)
                                duplicate_checking(hbogo_data,id_,initialization_file.source,writer,
                                     writer_credit_match_false,show_type,object_series,object_duplicate)
                        elif show_type=='MO':
                            if id not in mo_list:
                                mo_list.append(id)
                                duplicate_checking(hbogo_data,id_,initialization_file.source,writer,
                                     writer_credit_match_false,show_type,object_movies,object_duplicate)
                    print("\n")                             
                    print ({"Total SM":len(sm_list),"Total MO":len(mo_list),"Thread_name":thread_name})
                    print("\n")
                    print datetime.datetime.now()
            except (socket.error,Exception) as e:
                print ("exception caught.............",type(e),thread_name)
                continue
                                    
#TODO: create threads                
def thread_pool():

    t1=threading.Thread(target=main,args=(1,"thread-1",2000,1))
    t1.start()
    t2=threading.Thread(target=main,args=(2000,"thread-2",4000,2))
    t2.start()
    t3=threading.Thread(target=main,args=(4000,"thread-3",6000,3))
    t3.start()
    t4=threading.Thread(target=main,args=(6000,"thread-4",6884,4))
    t4.start()

#starting
thread_pool()
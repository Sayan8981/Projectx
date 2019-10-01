""" Here we are checking duplicate programs in search api """
"""Saayan"""

import threading
import pymongo
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
import datetime
import urllib2
import json
import httplib
import unidecode
import pinyin
sys.setrecursionlimit(1500)
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file
import Duplicate_checking_Movies
import Duplicate_checking_Series

def duplicate_checking_movies(i,source,writer,writer_credit_match_false):
    #import pdb;pdb.set_trace()
    object_movies=Duplicate_checking_Movies.ProjectXMovies()
    object_movies.cleanup()
    print("\n")
    print("Checking duplicates of Movies to Projectx search api................")
    print("\n")
    object_movies.duplicate_checking_movies(i,source,writer,writer_credit_match_false)  


def duplicate_checking_episodes(i,source,writer,writer_credit_match_false):
    #import pdb;pdb.set_trace()
    object_series=Duplicate_checking_Series.ProjectXSeries()
    object_series.cleanup()
    print("\n")
    print("Checking duplicates of Episodes to Projectx search api................")
    print("\n")   
    object_series.duplicate_checking_series(i,source,writer,writer_credit_match_false)
         
def create_csv(result_sheet):
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)    
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    output_file=open(os.getcwd()+result_sheet,"wa")
    return output_file         
                                
def main_func_movies(start_id,thread_name,end_id,page_id,movies_table,source):
    fieldnames_tag = ["source","sm_id","id","launch_id","show_type","movie_title","series_title","episode_title","release_year","link_expired","Projectx_ids","Variant id present for episode","Variant parent id for Episode","Projectx_series_id","comment","Result","Gb_id duplicate",'Duplicate ids in search',"Credit_match","Rovi_id count for duplicate","Gb_id count for duplicate","%s_id count for duplicate"%source,"Mapped rovi_id for duplicate","Mapped Gb_id for duplicate","Mapped %s_id for duplicate"%source,"variant parent id present","variant_parent_id","link_fetched_from","Sources of link"] 
    fieldnames_credit_match_false = ["px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary","px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary","px_id2_record_language","px_id1_credits_null","px_id1_db_credit_present","px_id2_credits_null","px_id2_db_credit_present","long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","Comment"]
    result_sheet_credit_match_false='/output1/duplicate_programs_credit_match_false_result_MO%d.csv'%page_id
    output_file_credit_match_false=create_csv(result_sheet_credit_match_false)
    result_sheet='/output/duplicate_checked_in_search_api_mo%d.csv'%page_id
    output_file=create_csv(result_sheet)
    with output_file as mycsvfile, output_file_credit_match_false as mycsvfile1:
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames_tag)
        writer_credit_match_false = csv.writer(mycsvfile1,dialect="excel",lineterminator = '\n')
        writer_credit_match_false.writerow(fieldnames_credit_match_false)  
        print("Checking duplicates of Movies to Projectx search api................")
        mo_list=[]
        for aa in range(start_id,end_id,100):
            try: 
                print ("episodes_table.aggregate([{'$skip':%d},{'$limit':100}")%aa
                query=initialization_file.movies_table.aggregate([{"$match":{"programming_type":"Full Movie"}},{"$project":{"id":1,"_id":0,"title":1,"programming_type":1,"type":1,"site_id":1,"original_premiere_date":1}},{"$skip":aa},{"$limit":100}])
                print ("\n")
                print({"start": start_id,"end":end_id})
                for i in query:
                    sm_id=''
                    id=i.get("site_id")
                    show_type=i.get("programming_type").encode('utf-8')
                    if show_type=='Full Movie':
                        show_type='MO'
                    if id is not None:
                        if show_type=="MO":
                            if id not in mo_list:
                                mo_list.append(id)
                                duplicate_checking_movies(i,source,writer,writer_credit_match_false)
                    print("\n")                             
                    print ({"Total MO":len(mo_list),"Thread_name":thread_name})
                    print("\n")
                    print datetime.datetime.now()
            except (Exception,pymongo.errors.CursorNotFound) as e:
                print ("exception caught................",type(e),thread_name)
                continue   
               

def main_func_episodes(start_id,thread_name,end_id,page_id,episodes_table,source):
    fieldnames_tag = ["source","sm_id","id","launch_id","show_type","movie_title","series_title","episode_title","release_year","link_expired","Projectx_ids","Variant id present for episode","Variant parent id for Episode","Projectx_series_id","comment","Result","id duplicate",'Duplicate ids in search',"Credit_match","Rovi_id count for duplicate","Gb_id_count for duplicate","%s_id count for duplicate"%source,"Mapped rovi_id for duplicate","Mapped gb_id for duplicate","Mapped %s_id for duplicate"%source,"variant parent id present","variant_parent_id","link_fetched_from","Sources of link"]
    fieldnames_credit_match_false = ["px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary","px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary","px_id2_record_language","long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","Comment"]
    result_sheet_credit_match_false='/output1/duplicate_programs_credit_match_false_result_SE%d.csv'%page_id
    output_file_credit_match_false=create_csv(result_sheet_credit_match_false)
    result_sheet='/output/duplicate_checked_in_search_api_SM%d.csv'%page_id
    output_file=create_csv(result_sheet)
    with output_file as mycsvfile, output_file_credit_match_false as mycsvfile1:
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames_tag)
        writer_credit_match_false = csv.writer(mycsvfile1,dialect="excel",lineterminator = '\n')
        writer_credit_match_false.writerow(fieldnames_credit_match_false)                                      
        print("Checking duplicates of Episodes to Projectx search api................")
        se_list=[]
        sm_list=[]
        for aa in range(start_id,end_id,100):
            #import pdb;pdb.set_trace()
            try:
                print ("episodes_table.aggregate([{'$skip':%d},{'$limit':100}")%aa
                query=initialization_file.episodes_table.aggregate([{"$match":{"programming_type":"Full Episode"}},{"$project":{"id":1,"_id":0,"title":1,"series.name":1,"programming_type":1,"type":1,"site_id":1,"original_premiere_date":1,"series.original_premiere_date":1,"series.site_id":1}},{"$skip":aa},{"$limit":100}])
                print ("\n")
                print({"start": start_id,"end":end_id})
                for i in query:
                    id=i.get("site_id")
                    series_id=i.get("series").get("site_id")
                    show_type=i.get("programming_type").encode('utf-8')
                    if show_type=='Full Episode':
                        show_type='SE'
                    #import pdb;pdb.set_trace()
                    if id is not None:
                        if show_type=="SE":
                            if id not in se_list:
                                se_list.append(id)
                                if series_id not in sm_list:
                                    sm_list.append(series_id)
                                    duplicate_checking_episodes(i,source,writer,writer_credit_match_false)
                    print("\n")                             
                    print ({"Total SE":len(se_list),"Total SM":len(sm_list),"Thread_name":thread_name})
                    print datetime.datetime.now()                 
            except (Exception,pymongo.errors.CursorNotFound) as e:
                print ("exception caught................",type(e),thread_name)
                continue 

#TODO: create threads                     
def threading_pool():    

    t1=threading.Thread(target=main_func_episodes,args=(0,"thread-1",20000,1,initialization_file.episodes_table,initialization_file.source))
    t1.start()
    t2=threading.Thread(target=main_func_episodes,args=(20000,"thread-2",40000,2,initialization_file.episodes_table,initialization_file.source))
    t2.start()
    t3=threading.Thread(target=main_func_episodes,args=(40000,"thread-3",60000,3,initialization_file.episodes_table,initialization_file.source))
    t3.start()
    t4=threading.Thread(target=main_func_episodes,args=(60000,"thread-4",100000,4,initialization_file.episodes_table,initialization_file.source))
    t4.start()
    t5=threading.Thread(target=main_func_movies,args=(0,"thread-5",1500,5,initialization_file.movies_table,initialization_file.source))
    t5.start()
    
    initialization_file.connection.close()

#starting
threading_pool()


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
sys.setrecursionlimit(1500)
import pinyin
sys.path.insert(0,os.getcwd()+'/lib')
import Duplicate_checking_Movies
import Duplicate_checking_Series
import initialization_file
#import pdb;pdb.set_trace()

#TODO: to open file to write O/P
def create_csv(result_sheet):    
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    output_file=open(os.getcwd()+result_sheet,"wa")
    return output_file

def main(start_id,thread_name,end_id,page_id):
    result_sheet_credit_match_false='/output1/duplicate_programs_credit_match_false_result%d.csv'%page_id
    output_file_credit_match_false=create_csv(result_sheet_credit_match_false)
    result_sheet='/output/duplicate_checked_in_search_api%d.csv'%page_id
    output_file=create_csv(result_sheet)
    fieldnames_tag = ["source","id","SM_id","show_type","link_details","movie_title","series_title","episode_title","release_year","link_expired","Projectx_ids","Variant id present for episode","Variant parent id for Episode","Projectx_series_id","comment","Result","Gb_id duplicate",'Duplicate ids in search',"Credit_match","Rovi_id count for duplicate","Gb_id count for duplicate","%s_id count for duplicate"%initialization_file.source,"Mapped rovi_id for duplicate","Mapped Gb_id for duplicate","Mapped %s_id for duplicate"%initialization_file.source,"variant_parent_id_present","variant_parent_id","link_fetched_from","Sources of link"]
    fieldnames_credit_match_false = ["px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary","px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary","px_id2_record_language","px_id1_credits_null","px_id1_db_credit_present","px_id2_credits_null","px_id2_db_credit_present","long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","Comment"]
    
    with output_file as mycsvfile, output_file_credit_match_false as mycsvfile1:
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames_tag)
        writer_credit_match_false = csv.writer(mycsvfile1,dialect="excel",lineterminator = '\n')
        writer_credit_match_false.writerow(fieldnames_credit_match_false)
        print("Checking duplicates of series and Movies to Projectx search api................",thread_name)
        #import pdb;pdb.set_trace()
        mo_list=[]
        sm_list=[]
        object_movies=Duplicate_checking_Movies.ProjectXMovies()
        object_series=Duplicate_checking_Series.ProjectXSeries()
        for aa in range(start_id,end_id,1000):
            querylist=[]
            try:
                query=initialization_file.mytable.aggregate([{"$match":{"$and":[{"show_type":{"$in":["SE","MO"]}}]}},{"$project":{"gb_id":1,"show_id":1,"_id":0,"show_type":1,'title':1,"series_title":1}},{"$skip":aa},{"$limit":1000}]) 
                print ("\n")
                print({"start_id": start_id,"end":end_id})
                querylist=[data for data in query]
                for data in querylist:
                    #import pdb;pdb.set_trace()
                    id_=data.get("gb_id")
                    sm_id=data.get("show_id")
                    show_type=data.get("show_type").encode('utf-8')
                    if id_ is not None:
                        if show_type=='SE':
                            if sm_id not in sm_list:
                                sm_list.append(sm_id)
                                #import pdb;pdb.set_trace()
                                object_series.cleanup()
                                object_series.duplicate_checking_series(data,initialization_file.source,writer,writer_credit_match_false)
                        elif show_type=='MO':
                            if id_ not in mo_list:
                                mo_list.append(id_)
                                object_movies.cleanup()
                                object_movies.duplicate_checking_movies(data,initialization_file.source,writer,writer_credit_match_false)
                    print("\n")                             
                    print ({"Total MO":len(mo_list),"Total SM":len(sm_list),"Thread_name":thread_name,"gb_id":id_})
                    print("\n")
                    print datetime.datetime.now()
            except (Exception,pymongo.errors.CursorNotFound) as e:
                print ("exception caught.............",type(e),thread_name)
                continue
                             

#TODO: to create threads
def thread_pool():    
    t1=threading.Thread(target=main,args=(0,"thread - 1",50000,1))
    t1.start()
    t2=threading.Thread(target=main,args=(100000,"thread - 2",200000,2))
    t2.start()                          
    t3=threading.Thread(target=main,args=(200000,"thread - 3",300000,3))
    t3.start()
    t4=threading.Thread(target=main,args=(400000,"thread - 4",500000,4))
    t4.start()
    t5=threading.Thread(target=main,args=(500000,"thread - 5",600000,5))
    t5.start()
    t6=threading.Thread(target=main,args=(600000,"thread - 6",700000,6))
    t6.start()
    t7=threading.Thread(target=main,args=(700000,"thread - 7",800000,7))
    t7.start()
    t8=threading.Thread(target=main,args=(800000,"thread - 8",900000,8))
    t8.start()
    t9=threading.Thread(target=main,args=(50000,"thread - 9",100000,9))
    t9.start()

    initialization_file.connection.close()

#starting
thread_pool()    

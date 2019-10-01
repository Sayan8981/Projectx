"""Writer: Saayan"""

import threading
import csv
import datetime
import sys
import urllib2
import json
import pymongo
import os
from urllib2 import URLError,HTTPError
import httplib
import socket
import pinyin
import unidecode
import time
from time import gmtime, strftime
sys.setrecursionlimit(15000)
sys.path.insert(0,os.getcwd()+'/common_lib')
sys.path.insert(1,os.getcwd()+'/common_lib/Px_lib')
sys.path.insert(2,os.getcwd()+'/common_lib/source_lib')
sys.path.insert(3,os.getcwd()+'/common_lib/validation_lib')
import initialization_file
import checking_only_mapped_px_id
import getting_meta
import meta_data_validation
import images_validation 
import ott_validation


"""
projectx_details={"px_credits":self.px_credits,"px_credit_present":self.px_credit_present,"px_long_title":self.px_long_title,"px_episode_title":self.px_episode_title,
                           "px_original_title":self.px_original_title,"px_description":self.px_description,"px_genres":self.px_genres,"px_aliases":self.px_aliases,
                            "px_release_year":self.px_release_year,"px_run_time":self.px_run_time,"px_season_number":self.px_season_number,"px_episode_number":self.px_episode_number,
                            "px_video_link_present":self.px_video_link_present,"px_images_details":self.px_images_details,"launch_id":self.launch_id}

source_details={"source_credits":self.hulu_credits,"source_credit_present":self.hulu_credit_present,"source_title":self.hulu_title,"source_description":self.hulu_description,
                    "source_genres":self.hulu_genres,"source_alternate_titles":self.hulu_alternate_titles,"source_release_year":self.hulu_release_year
                   ,"source_duration":self.hulu_duration,"source_season_number":self.hulu_season_number,"source_episode_number":self.hulu_episode_number,
                   "source_link_present":self.hulu_link_present,"source_images_details":self.hulu_images_details} 

meta_data_validation_result={"title_match":self.title_match,"description_match":self.description_match,"genres_match":self.genres_match,"release_year_match":self.release_year_match,
                "season_number_match":self.season_number_match,"episode_number_match":self.episode_number_match,"px_video_link_present":self.px_video_link_present,
                "source_link_present":self.source_link_present}
"""

#TODO: to validate the meta data population and printing to o/p sheet
def validation_result(source_id,show_type,data,hulu_table,projectx_id,source,thread_name,writer,object_):
    #import pdb;pdb.set_trace()
    object_.__init__()
    source_details=object_.getting_source_details(source_id,show_type,source,thread_name,data,hulu_table)
    projectx_details=object_.getting_projectx_details(projectx_id,show_type,source,thread_name)
    if projectx_details!='Null':
        #TODO: creating object for meta data validation module class
        meta_validation_object_=meta_data_validation.meta_data_validate()
        meta_validation_object_.__init__() 

        meta_data_validation_result=meta_validation_object_.meta_data_validation(source_id,source_details,projectx_details,show_type)

        if show_type=='MO' or show_type=='SE':
            ott_validation_result=ott_validation.ott_validation(projectx_details,data.get("id"))
        images_validation_result=images_validation.images_validation(source_details,projectx_details)
        try:
            writer.writerow([source_id,data.get("id"),projectx_id,show_type,source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"],meta_data_validation_result["release_year_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],meta_data_validation_result["source_link_present"],
            images_validation_result[0],images_validation_result[1],ott_validation_result,'',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),"Local: "+strftime("%Y-%m-%d %data:%M:%S %p")])
        except Exception:
            #import pdb;pdb.set_trace()
            writer.writerow([source_id,data.get("id"),projectx_id,show_type,source_details["source_title"],projectx_details["px_long_title"],projectx_details["px_episode_title"],meta_data_validation_result["title_match"],meta_data_validation_result["description_match"],meta_data_validation_result["genres_match"],meta_data_validation_result["release_year_match"],meta_data_validation_result["season_number_match"],meta_data_validation_result["episode_number_match"],meta_data_validation_result["px_video_link_present"],meta_data_validation_result["source_link_present"]
            ,images_validation_result[0],images_validation_result[1]])
    else:
        writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','','','','','','','','','','','','','','','','','Px_response_null','',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),"Local: "+strftime("%Y-%m-%d %data:%M:%S %p")])        

# TODO: going for meta data validation for movies
def meta_data_validation_mo(data,projectx_id,source_id,thread_name,writer,source,db_table):
    #import pdb;pdb.set_trace()
    retry_count=0
    object_mo=getting_meta.meta_data()
    object_mo.__init__()
    try:
        hulu_id=data.get("site_id")
        show_type=data.get("programming_type")
        if show_type=="Full Movie":
            show_type='MO'        
        only_mapped_ids=checking_only_mapped_px_id.getting_mapped_px_id(hulu_id,show_type,source,db_table)
        #if only_mapped_ids is not None:
        if only_mapped_ids[2]=='True':
            initialization_file.total_mo+=1
            #import pdb;pdb.set_trace()
            projectx_id=only_mapped_ids[0]
            source_id=only_mapped_ids[1]
            print("\n")
            print({"total_mo":initialization_file.total_mo,"source_mo_id":hulu_id,"thread_name":thread_name,"Px_id":projectx_id,"%s_id"%source:source_id})
            validation_result(source_id,show_type,data,db_table,projectx_id,source,thread_name,writer,object_mo)
        elif only_mapped_ids[2]=='True(Rovi+others)':
            #TODO: to check only OTT link population for the episodes
            #import pdb;pdb.set_trace()
            projectx_id=only_mapped_ids[0]
            source_id=only_mapped_ids[1]
            initialization_file.total_mo+=1
            print("\n")
            print ({"total_mo":initialization_file.total_mo,"Px_id":projectx_id,"%s_id"%source:source_id,"thread_name":thread_name})
            projectx_details=object_mo.getting_projectx_details(projectx_id,show_type,source,thread_name)
            if projectx_details!='Null':
                if data.get("id") is not None:
                    ott_validation_result=ott_validation.ott_validation(projectx_details,data.get("id"))
                    try:
                        writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','','','','','','','','','','','',ott_validation_result,only_mapped_ids[2],only_mapped_ids[3],'',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),"Local: "+strftime("%Y-%m-%d %data:%M:%S %p")])
                    except Exception as e:
                        print ("Exception caught ....................",type(e),hulu_id,show_type)
                        pass
            else:
                writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','','','','','','','','','','','','','',only_mapped_ids[2],only_mapped_ids[3],'Px_response_null','',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),"Local: "+strftime("%Y-%m-%d %data:%M:%S %p")])    
        else:
            writer.writerow([hulu_id,'','',show_type,'','','','','','','','','','','','','','','','','','',only_mapped_ids,'','Px_id_null','',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),"Local: "+strftime("%Y-%m-%d %data:%M:%S %p")])
    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
        retry_count+=1
        print("\n")
        print("Retrying...................................",retry_count)
        print("\n")
        print ("exception/error caught in (validation_mo).........................",type(e),hulu_id,show_type,thread_name)
        if retry_count<=5:
            meta_data_validation_mo(data,projectx_id,source_id,thread_name,writer,source,db_table)        
        else:
            retry_count=0    

# TODO: going for meta data validation for episodes and series
def meta_data_validation_se_sm(data,db_table,projectx_id,source_id,array_sm,source,writer,thread_name):
    #import pdb;pdb.set_trace()
    retry_count=0
    object_se=getting_meta.meta_data()
    try:
        if data.get("site_id") is not None:
            #import pdb;pdb.set_trace()
            hulu_id_se=data.get("site_id")
            show_type=data.get("programming_type")
            if show_type=="Full Episode":
                show_type='SE'
            only_mapped_ids_se=checking_only_mapped_px_id.getting_mapped_px_id(hulu_id_se,show_type,source,db_table)
            #if only_mapped_ids_se is not None:
            if only_mapped_ids_se[2]=='True':
                initialization_file.total_se+=1
                #import pdb;pdb.set_trace()
                projectx_id=only_mapped_ids_se[0]
                source_id=only_mapped_ids_se[1]
                print("\n")
                print({"total_se":initialization_file.total_se,"_hulu_se_id":hulu_id_se,"thread_name":thread_name,"Px_id":projectx_id,"%s_id"%source:source_id})
                validation_result(source_id,show_type,data,db_table,projectx_id,source,thread_name,writer,object_se)
            elif only_mapped_ids_se[2]=='True(Rovi+others)':
                #TODO: to check only OTT link population for the movies
                #import pdb;pdb.set_trace()
                object_se.__init__()
                projectx_id=only_mapped_ids_se[0]
                source_id=only_mapped_ids_se[1]
                print("\n")
                print ({"Px_id":projectx_id,"%s_id"%source:source_id,"thread_name":thread_name})
                projectx_details=object_se.getting_projectx_details(projectx_id,show_type,source,thread_name)
                if projectx_details!='Null':
                    if data.get("id") is not None:
                        ott_validation_result=ott_validation.ott_validation(projectx_details,data.get("id"))
                        try:
                            writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','','','','','','','','','','','',ott_validation_result,only_mapped_ids_se[2],only_mapped_ids_se[3],'',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),"Local: "+strftime("%Y-%m-%d %data:%M:%S %p")])
                        except Exception as e:
                            print ("Exception caught ....................",type(e),hulu_id_se,show_type)
                            pass    
                else:
                    writer.writerow([source_id,data.get("id"),projectx_id,show_type,'','','','','','','','','','','','','','','','',only_mapped_ids_se[2],only_mapped_ids_se[3],'Px_response_null','',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),"Local: "+strftime("%Y-%m-%d %data:%M:%S %p")])
            else:
                writer.writerow([hulu_id_se,'','',show_type,'','','','','','','','','','','','','','','','','','',only_mapped_ids_se,'','Px_id_null','',"GMT: "+time.strftime("%Y-%m-%d %data:%M:%S %p", time.gmtime()),"Local: "+strftime("%Y-%m-%d %data:%M:%S %p")])        
            
        #TODO: for series
        if data.get("series").get("site_id") is not None:
            #import pdb;pdb.set_trace()
            hulu_id_sm=data.get("series").get("site_id")
            if hulu_id_sm not in array_sm:
                array_sm.append(hulu_id_sm)
                show_type='SM'
                only_mapped_ids_sm=checking_only_mapped_px_id.getting_mapped_px_id(hulu_id_sm,show_type,source,db_table)
                if only_mapped_ids_sm is not None:
                    if only_mapped_ids_sm[2]=='True':
                        initialization_file.total_sm+=1
                        #import pdb;pdb.set_trace()
                        projectx_id=only_mapped_ids_sm[0]
                        source_id=only_mapped_ids_sm[1]
                        print("\n")
                        print({"total_sm":initialization_file.total_sm,"source_sm_id":hulu_id_sm,"thread_name":thread_name,"Px_id":projectx_id,"%s_id"%source:source_id})
                        validation_result(source_id,show_type,data,db_table,projectx_id,source,thread_name,writer,object_se)        
            #print ({"Px_id":projectx_id,"%s_id"%source:source_id})
    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError,pymongo.errors.CursorNotFound) as e:
        retry_count+=1
        print ("Exception caught (meta_data_validation_se_sm)....................",type(e),data.get("site_id"),thread_name)
        print ("\n")
        print ("retrying..................",retry_count)
        if retry_count<=5:
            meta_data_validation_se_sm(data,db_table,projectx_id,source_id,array_sm,source,writer,thread_name)
        else:
            retry_count=0    

def create_csv(result_sheet):
    #import pdb;pdb.set_trace()
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
    output_file=open(os.getcwd()+result_sheet,"wa")
    return output_file                    

#TODO: to open file for writing o/p
def main(start_id,thread_name,end_id,page_id,db_table,source):
    #import pdb;pdb.set_trace()
    result_sheet='/result/hulu_meta_data_checking%s.csv'%page_id
    output_file=create_csv(result_sheet)
    with output_file as mycsvfile:
        fieldnames_tag = ["%s_id"%source,"launch_id","Projectx_id","show_type","%s_title"%source,"Px_title","Px_episode_title","title_match","description_match",
                     "genres_match","release_year_match","season_number_match","episode_number_match","px_video_link_present","%s_link_present"%source,
                                                                                                  "image_url_missing","Wrong_url","ott_link_result"]
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames_tag)
        #query_mo(start_id,thread_name,end_id,db_table,source,writer)
        projectx_id=0
        source_id=0
        #import pdb;pdb.set_trace()
        for aa in range(start_id,end_id,1000):
            try:
                query_hulu=db_table.aggregate([{"$match":{"programming_type" : "Full Movie"}},{"$project":{"site_id":1,"_id":0,"programming_type":1,"series.site_id":1,"title":1,"series.original_premiere_date":1,"duration":1,"thumbnails":1,"url":1,"series.description":1,"link":1,"id":1,"original_premiere_date":1,"description":1,"availability.svod.end":1}},{"$skip":aa},{"$limit":1000}])
                for data in query_hulu:
                    if data.get("availability").get("svod").get("end") is None or data.get("availability").get("svod").get("end") > datetime.datetime.now().isoformat():
                        if data.get("site_id")!="":
                            #import pdb;pdb.set_trace()
                            initialization_file.count_hulu_id_mo+=1
                            print("\n")    
                            print("count_hulu_id:",initialization_file.count_hulu_id_mo,"name:",thread_name)
                            meta_data_validation_mo(data,projectx_id,source_id,thread_name,writer,source,db_table)
            except (Exception,pymongo.errors.OperationFailure,pymongo.errors.CursorNotFound,RuntimeError) as e:
                print ("Exception in query...........",type(e))
                pass                
        if end_id>1000:
            #query_se_sm(start_id,thread_name,end_id,db_table,source,writer)                       
            array_sm=[]
            #import pdb;pdb.set_trace()
            for aa in range(start_id,end_id,100):
                try:
                    query_hulu=db_table.aggregate([{"$match":{"programming_type" : "Full Episode","video_language" : "en"}},{"$project":{"site_id":1,"_id":0,"programming_type":1,"series.site_id":1,"title":1,"series.name":1,"series.original_premiere_date":1,"original_premiere_date":1,"duration":1,"thumbnails":1,"url":1,"series.description":1,"description":1,"number":1,"season.number":1,"link":1,"id":1,"series.series_art":1,"availability.svod.end":1}},{"$skip":aa},{"$limit":100}])
                    for data in query_hulu:
                        if data.get("availability").get("svod").get("end") is None or data.get("availability").get("svod").get("end") > datetime.datetime.now().isoformat():   
                            #import pdb;pdb.set_trace()
                            print("\n")
                            initialization_file.count_hulu_id_se+=1    
                            print("count_hulu_id_se:",initialization_file.count_hulu_id_se,"name:",thread_name)
                            meta_data_validation_se_sm(data,db_table,projectx_id,source_id,array_sm,source,writer,thread_name)
                        else:
                            initialization_file.Not_valid_programs_count+=1
                            print({"Not_valid_programs_count":initialization_file.Not_valid_programs_count,"thread":thread_name,"id":data.get("site_id")})
                except (Exception,pymongo.errors.OperationFailure,pymongo.errors.CursorNotFound,RuntimeError) as e:
                    print ("Exception in query...........",type(e))
                    pass            

#TODO : to set up threads
def threading_pool():    

    t1=threading.Thread(target=main,args=(1,"thread-1",2000,'MO',initialization_file.mytable_movies,initialization_file.source))
    t1.start()
    t2=threading.Thread(target=main,args=(0,"thread-2",20000,'SE1',initialization_file.mytable_episodes,initialization_file.source))
    t2.start()
    t3=threading.Thread(target=main,args=(22000,"thread-3",40000,'SE2',initialization_file.mytable_episodes,initialization_file.source))
    t3.start()
    t4=threading.Thread(target=main,args=(40000,"thread-4",60000,'SE3',initialization_file.mytable_episodes,initialization_file.source))
    t4.start()
    t5=threading.Thread(target=main,args=(60000,"thread-5",80000,'SE4',initialization_file.mytable_episodes,initialization_file.source))
    t5.start()
    t6=threading.Thread(target=main,args=(80000,"thread-6",100000,'SE5',initialization_file.mytable_episodes,initialization_file.source))
    t6.start()

    initialization_file.connection.close()  

# Starting     
threading_pool()


#220570
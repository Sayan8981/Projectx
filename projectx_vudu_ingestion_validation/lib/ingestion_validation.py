import sys
import csv
import os
import datetime
import urllib2
import json
from urllib2 import HTTPError
import httplib
import socket
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file

#TODO: to fetch the response from API
def fetch_from_cloud_for_id(api):
    resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':initialization_file.token}))
    data = resp.read()
    data_resp = json.loads(data)
    return data_resp 

#TODO: to get duplicate source_ids
def getting_duplicate_source_id(px_id,px_mapping_api,token,source,show_type):
    source_list=[]
    #import pdb;pdb.set_trace()
    for _id in px_id:
        resp=urllib2.urlopen(urllib2.Request(px_mapping_api%_id,None,{'Authorization':token}))
        data_=resp.read()
        data_resp_mapping=json.loads(data_)

        for resp in data_resp_mapping:
            if resp.get("data_source")==source and resp.get("sub_type")==show_type and resp.get("type")=='Program':
                source_list.append(resp.get("sourceId"))
    return source_list        

#TODO: to do validate the ingestion 
def validation_ingestion(_id,_projectx_id,show_type,source,title,release_year,
                                       series_title,episode_number,language,writer,thread_name):
    #import pdb;pdb.set_trace()
    retry_count=0
    try:
        duplicate_present=''
        duplicate_px_id=[]
        if _projectx_id:
            if len(_projectx_id)>1:
                initialization_file.multiple_mapped_count+=1
                writer.writerow({"%s_id"%source:_id,"show_type":show_type,"title":title,"release_year":release_year,"series_title":series_title,
                             "episode_number":episode_number,"language":language,"projectx_id_%s"%source:_projectx_id,"Comment":'Multiple ingestion for same content'})
            elif len(_projectx_id)==1:
                preprod_resp_api=initialization_file.projectx_preprod_domain%(_projectx_id[0])
                data_resp=fetch_from_cloud_for_id(preprod_resp_api)
                if data_resp==[]:
                    initialization_file.px_response='Null'
                initialization_file.pass_count+=1
                writer.writerow({"%s_id"%source:_id,"show_type":show_type,"title":title,"release_year":release_year,"series_title":series_title,
                            "episode_number":episode_number,"language":language,"projectx_id_%s"%source:_projectx_id,"Comment":'Ingested',"px_response":initialization_file.px_response})     
        else:
            duplicate_api=initialization_file.duplicate_api%(_id,source,show_type)
            resp=urllib2.urlopen(urllib2.Request(duplicate_api,None,{'Authorization':initialization_file.token}))
            data_=resp.read()
            data_resp_duplicate=json.loads(data_)
            if data_resp_duplicate!=[]:
                duplicate_present='True'
                for px_id in data_resp_duplicate:
                    duplicate_px_id.append(px_id.get("projectxId"))
                source_id_duplicate=getting_duplicate_source_id(duplicate_px_id,initialization_file.px_mapping_api,initialization_file.token,source,show_type)    
                writer.writerow({"%s_id"%source:_id,"show_type":show_type,"title":title,"release_year":release_year,"series_title":series_title,
                                "episode_number":episode_number,"language":language,"projectx_id_%s"%source:'',"Comment":'Not Ingested',"duplicate_response_present":duplicate_present,
                                                                                                 "duplicate_%s_id"%source:source_id_duplicate})
            else:    
                initialization_file.not_ingested_count+=1
                writer.writerow({"%s_id"%source:_id,"show_type":show_type,"title":title,"release_year":release_year,"series_title":series_title,
                                    "episode_number":episode_number,"language":language,"projectx_id_%s"%source:'',"Comment":'Not Ingested'})    
        print ("\n")        
        print("thread name:", thread_name,"Not ingested: ", initialization_file.not_ingested_count, "Multiple mapped content :", initialization_file.multiple_mapped_count, 
                                "Total Fail: ", initialization_file.not_ingested_count+initialization_file.multiple_mapped_count, 
                                                           "Pass: ", initialization_file.pass_count,"px_response:",initialization_file.px_response)
        print ("\n")  
        print(datetime.datetime.now())
    except (Exception,HTTPError,urllib2.URLError,socket.error) as e:
        retry_count+=1
        print ("Exception caught............",type(e),_id,_projectx_id,thread_name)
        print ("retrying..............................")
        if retry_count<=5:
            validation_ingestion(_id,_projectx_id,show_type,source,title,release_year,
                                 series_title,episode_number,language,writer,thread_name)
        else:
            retry_count=0

    
 


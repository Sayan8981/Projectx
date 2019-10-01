"""Writer: Saayan"""

import threading
import csv
import os
import pymongo
import datetime
import sys
import urllib2
import json
from urllib2 import HTTPError
import httplib
import socket
import pinyin
import unidecode
sys.path.insert(0,os.getcwd()+'/lib')
import ingestion_validation
import initialization_file

#TODO: to get projectx_ids from mapping api
def getting_px_ids(_id,show_type,thread_name,source):            
    #import pdb;pdb.set_trace()
    retry_count=0
    projectx_id=[]
    try:
        url_id=initialization_file.source_mapping_api%(_id,show_type)
        resp_id=urllib2.urlopen(urllib2.Request(url_id,None,{'Authorization':initialization_file.token}))
        data_id=resp_id.read()
        data_resp_id=json.loads(data_id)
        if data_resp_id:
            for jj in data_resp_id:
                if jj["data_source"]==source and jj["type"]=="Program" and jj["sub_type"]==show_type:
                    projectx_id.append(jj["projectxId"])
                    return projectx_id        
    except (Exception,HTTPError,urllib2.URLError,socket.error) as e:
        retry_count+=1
        print ("Exception caught............",type(e),_id,thread_name)
        print ("retrying..............................")
        if retry_count<=5:
            getting_px_ids(_id,show_type,thread_name,source)
        else:
            retry_count=0

#TODO: to validate the ingestion of source_ids
def IngestionValidation(_id,show_type,title,release_year,series_title,episode_number,language,writer,thread_name,source):
    #TODO: to get Px_id ids for source_id
    px_id=getting_px_ids(_id,show_type,thread_name,source) 
    ingestion_validation.validation_ingestion(_id,px_id,show_type,source,title,release_year,series_title,episode_number,language,writer,thread_name)

#TODO: to get source_details 
def getting_source_details(data): 
    _id=data.get("launch_id").encode()
    show_type=data.get("show_type").encode()
    print ({"vudu_id":_id,"show_type":show_type})
    title=unidecode.unidecode(pinyin.get(unicode(data.get("title"))))
    release_year=data.get("release_year")
    series_title=unidecode.unidecode(pinyin.get(unicode(data.get("series_title"))))
    episode_number=data.get("episode_number")
    language=unidecode.unidecode(pinyin.get(unicode(data.get("language"))))
    return {"_id":_id,"show_type":show_type,"title":title,"release_year":release_year,"series_title":series_title,
                          "episode_number":episode_number,"language":language}
#    IngestionValidation(_id,show_type,title,release_year,series_title,episode_number,language,writer,thread_name,source)
         
#TODO: to open file for writing O/P
def create_csv(result_sheet):
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
    output_file=open(os.getcwd()+result_sheet,"wa")
    return output_file

def main(start_id,thread_name,end_id,page_id,sourceidtable,fieldnames,source):
    print({"start":start_id,"end":end_id})    
    result_sheet='/output/Ingestion_checking_test_%s_id%d.csv'%(source,page_id)
    output_file=open(os.getcwd()+result_sheet,"wa")
    with output_file as mycsvfile:
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        #import pdb;pdb.set_trace()
        for aa in range(start_id,end_id,1000):
            try:
                id_query=sourceidtable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"$and":[{"show_type":{"$in":["MO","SE","SM"]}}]}},{"$project":{"launch_id":1,"_id":0,"show_type":1,"title":1,"release_year":1,"series_title":1,"episode_number":1,"language":1}}])
                for data in id_query:
                    details=getting_source_details(data)
                    IngestionValidation(details["_id"],details["show_type"],details["title"],details["release_year"],details["series_title"],
                                          details["episode_number"],details["language"],writer,thread_name,source)
            except (Exception,pymongo.errors.CursorNotFound) as e:
                pass        
    output_file.close()            

# TODO: creating threads and calling function main
def thread_pool(): 

    t1=threading.Thread(target=main,args=(0,"thread-1",20000,1,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t1.start()
    t2=threading.Thread(target=main,args=(20000,"thread-2",40000,2,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t2.start()
    t3=threading.Thread(target=main,args=(40000,"thread-3",60000,3,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t3.start()
    t4=threading.Thread(target=main,args=(60000,"thread-4",80000,4,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source,))
    t4.start()
    t5=threading.Thread(target=main,args=(80000,"thread-5",100000,5,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t5.start()
    t6=threading.Thread(target=main,args=(100000,"thread-6",120000,6,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t6.start()
    t7=threading.Thread(target=main,args=(120000,"thread-7",140000,7,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t7.start()
    t8=threading.Thread(target=main,args=(140000,"thread-8",160000,8,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t8.start()
    t9=threading.Thread(target=main,args=(160000,"thread-9",180000,9,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t9.start()
    t10=threading.Thread(target=main,args=(180000,"thread-10",200000,10,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t10.start()
    t11=threading.Thread(target=main,args=(200000,"thread-11",220000,11,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t11.start()
    t12=threading.Thread(target=main,args=(220000,"thread-12",240000,12,initialization_file.sourceidtable,initialization_file.fieldnames,initialization_file.source))
    t12.start()

    initialization_file.connection.close()

#starting
thread_pool()

"""Writer: Saayan"""

import threading
import sys
import urllib2
import json
import os
from urllib2 import URLError,HTTPError
import csv
import datetime
import httplib
import socket
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file
import checking_mapping_movies

def fetch_from_cloud_for_id(api):
    resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':initialization_file.Token}))
    data = resp.read()
    data_resp = json.loads(data)
    return data_resp

def getting_px_ids(source_id,rovi_id,thread_name,source):
    #import pdb;pdb.set_trace()
    retry_count=0
    source_projectx_id=[]
    rovi_projectx_id=[]
    try:
        url_rovi=initialization_file.rovi_mapping_api%eval(rovi_id)
        data_resp_rovi=fetch_from_cloud_for_id(url_rovi)
        url_source=initialization_file.source_mapping_api%(eval(source_id),source)
        data_resp_source=fetch_from_cloud_for_id(url_source)
        for ii in data_resp_rovi:
            if ii["data_source"]=='Rovi' and ii["type"]=='Program':
                rovi_projectx_id.append(ii["projectxId"])
        for jj in data_resp_source:
            if jj["data_source"]==source and jj["type"]=='Program' and jj["sub_type"]=='MO':
                source_projectx_id.append(jj["projectxId"])
        print("\n")        
        print ({"source_projectx_id":source_projectx_id, "rovi_projectx_id":rovi_projectx_id })
        return {"source_projectx_id":source_projectx_id, "rovi_projectx_id":rovi_projectx_id}
        #TODO: going for mapping checking
    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError) as e:
        retry_count+=1
        print("\n")
        print ("exception caught ............................................",type(e),thread_name,source_id,rovi_id)
        print("retrying,.............................",retry_count)
        if retry_count<=5:
            getting_px_ids(source_id,rovi_id,thread_name,source)
        else:
            retry_count=0    

def read_csv(inputFile):
    input_file = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(input_file)
    input_data=list(reader)    
    return input_data

#TODO: to write file
def create_csv(result_sheet):
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
    output_file=open(os.getcwd()+result_sheet,"wa")
    return output_file


#TODO: main func
def main(start_id,thread_name,end_id,page_id):
    inputFile="/input/hbogo_movie_mappings"
    result_sheet='/output/Result_mapping_MO_new_API%d.csv'%page_id
    input_data=read_csv(inputFile)
    output_file=create_csv(result_sheet)
    fieldnames_tag = ["%s_id"%initialization_file.source,"Rovi_id","projectx_id_rovi","projectx_id_%s"%initialization_file.source,"Dev_%s_px_mapped"%initialization_file.source,"Dev_rovi_px_mapped","another_rovi_id_present","another_%s_id_present"%initialization_file.source,"variant_present","Comment","Result of mapping"]
    retry_count=0
    with output_file as mycsvfile:
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames_tag,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        for r in range(start_id,end_id):
            #import pdb;pdb.set_trace()
            print("\n")
            print ("here checking the rovi and Vudu movies program mapping...................... ",thread_name)
            initialization_file.total=initialization_file.total+1
            hbogo_id=str(input_data[r][1])
            rovi_id=str(input_data[r][2])
            #import pdb;pdb.set_trace()
            try:
                px_ids=getting_px_ids(hbogo_id,rovi_id,thread_name,initialization_file.source)
                checking_mapping_movies.checking_mapping_movies(hbogo_id,rovi_id,px_ids["source_projectx_id"],
                        px_ids["rovi_projectx_id"],writer,thread_name,initialization_file.source)
            except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
                retry_count+=1
                print("\n")
                print ("Exception caught ......................",type(e),hbogo_id,rovi_id)
                if retry_count<=5:
                   px_ids= getting_px_ids(hbogo_id,rovi_id,thread_name,initialization_file.source)
                   checking_mapping_movies.checking_mapping_movies(hbogo_id,rovi_id,px_ids["source_projectx_id"],
                           px_ids["rovi_projectx_id"],writer,thread_name,initialization_file.source)
                else:
                    retry_count=0    

    output_file.close()        
    
#TODO: create threads            
def thread_pool():
 
    t1=threading.Thread(target=main,args=(1,'Thread -1',4000,1))
    t1.start()
    t2=threading.Thread(target=main,args=(4000,'Thread -2',8000,2))
    t2.start()
    t3=threading.Thread(target=main,args=(8000,'Thread -3',12000,3))
    t3.start()
    t4=threading.Thread(target=main,args=(12000,'Thread -4',16000,4))
    t4.start()
    t5=threading.Thread(target=main,args=(16000,'Thread -5',22018,5))
    t5.start()
#starting
thread_pool()   

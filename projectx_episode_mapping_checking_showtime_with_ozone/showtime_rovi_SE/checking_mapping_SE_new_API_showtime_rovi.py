"""Writer: Saayan"""

import threading
import sys
import urllib2
import json
from urllib2 import HTTPError,URLError
import csv
import os
import datetime
import httplib
import socket
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file
import checking_mapping_episodes

#TODO: to fetch response from API
def fetch_api_response(api):
    resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':initialization_file.Token}))
    data = resp.read()
    data_resp = json.loads(data)
    return data_resp

#TODO: getting Px_id from Mapping API
def getting_px_ids(source_id,rovi_id,thread_name,source):
    #import pdb;pdb.set_trace() 
    retry_count=0
    data_resp_source=''
    data_resp_rovi='' 
    source_projectx_id=[]
    rovi_projectx_id=[]
    try:
        url_rovi=initialization_file.rovi_mapping_api%rovi_id
        data_resp_rovi=fetch_api_response(url_rovi)
        url_source=initialization_file.source_mapping_api%(source_id,source,'SE')
        data_resp_source=fetch_api_response(url_source)    

        for ii in data_resp_rovi:
            if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                rovi_projectx_id.append(ii["projectxId"])
        for jj in data_resp_source:
             if jj["data_source"]==source and jj["type"]=="Program" and jj["sub_type"]=="SE":
                source_projectx_id.append(jj["projectxId"])    

        print ({"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id})
        return {"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id}
    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
        retry_count+=1
        print ("exception caught ..........................................",type(e),thread_name,source_id,rovi_id)
        print ("\n")
        print ("Retrying.............",retry_count)
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
        
#TODO: to read and write file O/P        
def main(start_id,thread_name,end_id,page_id):
    inputFile="/input/showtime_episode_mappings"
    #import pdb;pdb.set_trace()
    result_sheet='/result/Result_mapping_Episodes_API%d.csv'%page_id
    input_data=read_csv(inputFile)
    output_file=create_csv(result_sheet)
    fieldnames_tag = ["%s_sm_id"%initialization_file.source,"%s_id"%initialization_file.source,"Rovi_id","Rovi_SM_id","projectx_id_rovi","projectx_id_%s"%initialization_file.source,"Dev_%s_px_mapped"%initialization_file.source,"Dev_rovi_px_mapped","another_rovi_id_present","another_%s_id_present"%initialization_file.source,"series_id_match","Px_series_id","Episode mapping","Comment","gb_id_present","hulu_id_present"]
    retry_count=0
    with output_file as mycsvfile:
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames_tag,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        for data in range(start_id,end_id):
            #import pdb;pdb.set_trace()
            source_projectx_id_sm=[]
            initialization_file.total=initialization_file.total+1
            showtime_id=eval(input_data[data][2])
            rovi_id=eval(input_data[data][4])
            showtime_sm_id=eval(input_data[data][1])
            print("\n")
            print ("Total:", initialization_file.total, "thread_name:", thread_name,"start:",start_id,"end:",end_id,"showtime_sm_id:",showtime_sm_id,"showtime_id:",showtime_id,"rovi_id:",rovi_id)
            rovi_series_id=eval(input_data[data][3])
            try:
                px_ids=getting_px_ids(showtime_id,rovi_id,thread_name,initialization_file.source)
                checking_mapping_episodes.checking_mapping_episodes(showtime_id,rovi_id,showtime_sm_id,rovi_series_id,px_ids["rovi_projectx_id"],px_ids["source_projectx_id"],
                                           source_projectx_id_sm,writer,thread_name,initialization_file.source)
            except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
                retry_count+=1
                print ("Exception caught ......................",type(e),showtime_id,rovi_id)
                if retry_count<=5:
                    px_ids= getting_px_ids(showtime_id,rovi_id,thread_name,initialization_file.source)
                    checking_mapping_episodes.checking_mapping_episodes(showtime_id,rovi_id,showtime_sm_id,rovi_series_id,px_ids["rovi_projectx_id"],px_ids["source_projectx_id"],
                                               source_projectx_id_sm,writer,thread_name,initialization_file.source)
                else:
                    retry_count=0    

    output_file.close()        
  
#TODO: create threds
def thread_pool():            

    t1 =threading.Thread(target=main,args=(1,"thread - 1",30000,1))
    t1.start()
    t2 =threading.Thread(target=main,args=(32860,"thread - 2",60000,2))
    t2.start()
    t3 =threading.Thread(target=main,args=(60663,"thread - 3",90000,3))
    t3.start()
    t4 =threading.Thread(target=main,args=(90000,"thread - 4",108069,4))
    t4.start()

#starting
thread_pool()





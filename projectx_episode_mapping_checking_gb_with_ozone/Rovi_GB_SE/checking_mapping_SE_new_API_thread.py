"""Writer: Saayan"""

import threading
import socket
import sys
import urllib2
import json
import os
from urllib2 import HTTPError,URLError
import csv
import datetime
import httplib
sys.path.insert(0,os.getcwd()+'/common_lib')
import initialization_file
import checking_mapping_episodes

#TODO: fetch response from API
def fetch_api_response(api):
    link = urllib2.Request(api,None,{'Authorization':initialization_file.Token})
    resp = urllib2.urlopen(link)
    data = resp.read()
    data_resp = json.loads(data)
    return data_resp

def getting_px_ids(source_id,rovi_id,thread_name,source):
        #import pdb;pdb.set_trace()
        retry_count=0 
        data_resp_source=''
        data_resp_rovi='' 
        source_projectx_id=[]
        rovi_projectx_id=[]
        try:
            data_resp_rovi=fetch_api_response(initialization_file.rovi_mapping_api%rovi_id)
            data_resp_source=fetch_api_response(initialization_file.source_mapping_api %(source_id,source,'SE'))    
            for ii in data_resp_rovi:
                if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                    rovi_projectx_id.append(ii["projectxId"])
            for jj in data_resp_source:
                 if jj["data_source"]==source and jj["type"]=="Program" and jj["sub_type"]=="SE":
                    source_projectx_id.append(jj["projectxId"])    
            print("\n")
            print ({"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id})
            return {"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id}
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
            retry_count+=1
            print ("exception caught ...........................................",type(e),thread_name,source_id,rovi_id)
            print ("\n")
            print ("Retrying.............",retry_count)
            if retry_count<5:
                getting_px_ids(source_id,rovi_id,thread_name,source)
            else:
               retry_count =0   

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
   inputFile="/input/GB_Rovi_Mapping_episodes"
   #import pdb;pdb.set_trace()
   result_sheet='/result/Result_mapping_Episodes_API%d.csv'%page_id
   input_data=read_csv(inputFile)
   output_file=create_csv(result_sheet)
   fieldnames = ["%s_sm_id"%initialization_file.source,"%s_id"%initialization_file.source,"Rovi_id","Rovi_SM_id","Scheme","episode_title","ozoneepisodetitle","OzoneOriginalEpisodeTitle","projectx_id_rovi","projectx_id_%s"%initialization_file.source,"Dev_%s_px_mapped"%initialization_file.source,"Dev_rovi_px_mapped","another_rovi_id_present","another_%s_id_present"%initialization_file.source,"series_id_match","Px_series_id","Episode mapping","Comment"]
   retry_count=0
   with output_file as mycsvfile:
       writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
       writer.writeheader() 
       for data in range(start_id,end_id):
           source_projectx_id_sm=[]
           #import pdb;pdb.set_trace()
           initialization_file.total+=1
           print("\n")
           print ({"start":start_id,"end":end_id})
           gb_id=eval(input_data[data][4])
           rovi_id=eval(input_data[data][10])
           gb_sm_id=int(input_data[data][0])
           print("\n")
           print ("Total:", initialization_file.total,"gb_id:",gb_id,"rovi_id:",rovi_id,"gb_sm_id:",gb_sm_id, "thread_name:", thread_name)
           episode_title=str(input_data[data][7])
           ozoneepisodetitle=str(input_data[data][9])
           OzoneOriginalEpisodeTitle=str(input_data[data][8])
           scheme=input_data[data][11]
           rovi_series_id=input_data[data][14]
           try:
               px_ids=getting_px_ids(gb_id,rovi_id,thread_name,initialization_file.source)
               checking_mapping_episodes.checking_mapping_episodes(gb_id,rovi_id,gb_sm_id,episode_title,ozoneepisodetitle,OzoneOriginalEpisodeTitle,scheme,rovi_series_id,
                            px_ids["rovi_projectx_id"],px_ids["source_projectx_id"],source_projectx_id_sm,writer,thread_name,initialization_file.source)
           except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
               retry_count+=1
               print ("Exception caught ......................",type(e),gb_id,rovi_id)
               if retry_count<=5:
                  px_ids= getting_px_ids(gb_id,rovi_id,thread_name,initialization_file.source)
                  checking_mapping_episodes.checking_mapping_episodes(gb_id,rovi_id,gb_sm_id,episode_title,ozoneepisodetitle,OzoneOriginalEpisodeTitle,scheme,rovi_series_id,
                               px_ids["rovi_projectx_id"],px_ids["source_projectx_id"],source_projectx_id_sm,writer,thread_name,initialization_file.source)
               else:
                   retry_count=0    

   output_file.close()        

#TODO: create threads
def thread_pool():             

    t1 =threading.Thread(target=main,args=(1,"thread - 1",30001,1))
    t1.start()
    t2 =threading.Thread(target=main,args=(30001,"thread - 2",60001,2))
    t2.start()
    t3 =threading.Thread(target=main,args=(60001,"thread - 3",90001,3))
    t3.start()
    t4 =threading.Thread(target=main,args=(90001,"thread - 4",120001,4))
    t4.start()
    t5 =threading.Thread(target=main,args=(120001,"thread - 5",150001,5))
    t5.start()
    t6 =threading.Thread(target=main,args=(150001,"thread - 6",175001,6))
    t6.start()
    t7 =threading.Thread(target=main,args=(175001,"thread - 7",200001,7))
    t7.start()
    t8 =threading.Thread(target=main,args=(200001,"thread - 8",212485,8))
    t8.start()
#starting
thread_pool()




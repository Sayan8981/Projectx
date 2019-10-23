"""Writer: Saayan"""

import os
import datetime
import httplib
import sys
import urllib2
import json
from urllib2 import URLError,HTTPError
import csv
import httplib
import socket
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file
import checking_mapping_series

#TODO: to fetch response from  API
def fetch_api_response(api):
    resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':initialization_file.Token}))
    data = resp.read()
    data_resp = json.loads(data)
    return data_resp

def getting_px_ids(source_sm_id,rovi_sm_id,source):
    #import pdb;pdb.set_trace()
    retry_count=0
    source_projectx_id=[]
    rovi_projectx_id=[]
    try:
        url_rovi=initialization_file.rovi_mapping_api%rovi_sm_id
        data_resp_rovi=fetch_api_response(url_rovi)
        url_source=initialization_file.source_mapping_api%(source_sm_id,source,'SM')
        data_resp_source=fetch_api_response(url_source)

        for ii in data_resp_rovi:
            if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                rovi_projectx_id.append(ii["projectxId"])
        for jj in data_resp_source:
            if jj["data_source"]==source and jj["type"]=="Program" and jj["sub_type"]=="SM":
                source_projectx_id.append(jj["projectxId"])
        print("\n")
        print ({"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id})
        #TODO: return px_ids
        return {"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id}
    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
        retry_count+=1
        print ("Exception caught...................",type(e),source_sm_id,rovi_sm_id)
        if retry_count<=5: 
            getting_px_ids(source_sm_id,rovi_sm_id,source)
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
def main():
    inputFile="/input/hbogo_series_mapping"   
    result_sheet='/result/Result_mapping_SM.csv'
    fieldnames_tag = ["%s_sm_id"%initialization_file.source,"Rovi_sm_id","projectx_id_rovi","projectx_id_%s"%initialization_file.source,"Dev_%s_px_mapped"%initialization_file.source,"Dev_rovi_px_mapped","another_rovi_id_present","another_%s_id_present"%initialization_file.source,"variant_present","Comment","Result of mapping series"]
    input_data=read_csv(inputFile)
    output_file=create_csv(result_sheet)
    retry_count=0
    with output_file as mycsvfile:
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames_tag,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        for data in range(1,len(input_data)):
            initialization_file.total=initialization_file.total+1
            hbogo_sm_id=eval(input_data[data][0])
            rovi_sm_id=eval(input_data[data][1])
            print ({"total":initialization_file.total,"rovi_sm_id":rovi_sm_id,"hbogo_sm_id":hbogo_sm_id})
            try:
                px_ids=getting_px_ids(hbogo_sm_id,rovi_sm_id,initialization_file.source)
                checking_mapping_series.checking_mapping_series(hbogo_sm_id,rovi_sm_id,px_ids["source_projectx_id"],
                                 px_ids["rovi_projectx_id"],writer,initialization_file.source)
            except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
                #import pdb;pdb.set_trace()
                retry_count+=1
                print ("Exception caught ......................",type(e),hbogo_sm_id,rovi_sm_id)
                if retry_count<=5:
                   px_ids= getting_px_ids(hbogo_sm_id,rovi_sm_id,initialization_file.source)
                   checking_mapping_series.checking_mapping_series(hbogo_sm_id,rovi_sm_id,px_ids["source_projectx_id"],
                                    px_ids["rovi_projectx_id"],writer,initialization_file.source)
                else:
                    retry_count=0    

    output_file.close()


#starting
main()


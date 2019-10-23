"""Writer: Saayan"""

import sys
import urllib2
import json
from urllib2 import URLError,HTTPError
import csv
import os
import datetime
import httplib
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file

#TODO: To fetch response form API
def fetch_api_response(api):
    resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':initialization_file.Token}))
    data = resp.read()
    data_resp = json.loads(data)
    return data_resp

def to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source):
    another_source_id_present=''
    for mapped in data_mapped_resp:
        if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
            dev_rovi_px_mapped.append({"rovi_projectx_id_mapped":"rovi_id:"+str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})

        elif mapped.get("type")=="Program" and mapped.get("data_source")==source and mapped.get("sub_type")=='SE':
            dev_rovi_px_mapped.append({"source_id":str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})
            if mapped.get("sourceId")!=str(source_id):
                another_source_id_present='True'
            else:
                another_source_id_present='False' 
    return {"another_source_id_present":another_source_id_present,"dev_rovi_px_mapped":dev_rovi_px_mapped}

def to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source):
    another_rovi_id_present=''
    for mapped in data_mapped_resp_source:
        if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
            dev_source_px_mapped.append({"source_projectx_id_mapped":"rovi_id:"+str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})
            #TODO: taking decision
            if mapped.get("sourceId")!=str(rovi_id):
                another_rovi_id_present='True'
            else:
                another_rovi_id_present='False'

        elif mapped.get("type")=="Program" and mapped.get("data_source")==source and mapped.get("sub_type")=='SE':
            dev_source_px_mapped.append({"source_id":str(mapped.get("sourceId"))})                   

    return {"another_rovi_id_present":another_rovi_id_present,"dev_source_px_mapped":dev_source_px_mapped}         

def to_check_series_match(projectx_api_response,px_series_id):
    series_id_match='False'
    for kk in projectx_api_response:
        if kk.get("series_id") not in px_series_id:
            px_series_id.append(kk.get("series_id"))
        else:
            series_id_match='True'
    return {"series_id_match":series_id_match,"px_series_id":px_series_id}    

def checking_mapping_episodes(source_id,rovi_id,source_sm_id,rovi_series_id,rovi_projectx_id,source_projectx_id,source_projectx_id_sm,writer,thread_name,source):
    #import pdb;pdb.set_trace()
    dev_rovi_px_mapped=[]
    dev_source_px_mapped=[]
    px_series_id=[]
    px_array=[]
    #TODO: condition 1, to check both projectx ids are same or not
    if len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
        if rovi_projectx_id == source_projectx_id:
            initialization_file.mapped_count=initialization_file.mapped_count+1
            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                          "projectx_id_%s"%source:str(source_projectx_id[0]),"Episode mapping":'Pass',"Comment":'Pass'})

        else:
            #import pdb;pdb.set_trace()
            projectx_mapping_api=initialization_file.projectx_mapping_api_domain%rovi_projectx_id[0]
            data_mapped_resp=fetch_api_response(projectx_mapping_api)
            another_source_id_present_status=to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source)

            projectx_mapping_api=initialization_file.projectx_mapping_api_domain%source_projectx_id[0]
            data_mapped_resp_source=fetch_api_response(projectx_mapping_api)
            another_rovi_id_present_status=to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source)    

            px_array=[source_projectx_id[0],rovi_projectx_id[0]]
            projectx_api=initialization_file.projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
            projectx_api_response=fetch_api_response(projectx_api)
            series_id_match_status=to_check_series_match(projectx_api_response,px_series_id) 

            initialization_file.not_mapped_count=initialization_file.not_mapped_count+1
            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                            "projectx_id_%s"%source:str(source_projectx_id[0]),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                            "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"series_id_match":series_id_match_status["series_id_match"],
                                 "Px_series_id":series_id_match_status["px_series_id"],"Episode mapping":'Fail',"Comment":'Fail'})
    #TODO: condition 2 , to check multiple projectx ids for a source        
    elif len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
        if source_projectx_id and rovi_projectx_id:
            initialization_file.multiple_mapped_count=initialization_file.multiple_mapped_count+1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id[0] in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'
                else:
                    status='Pass'
                    comment='Fail:Multiple ingestion for same content of rovi'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of vudu'
                else:
                    status='Fail'
                    comment=   'Fail:Multiple ingestion for same content of vudu'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id: 
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        status='Fail'
                        comment= 'Fail:Multiple ingestion for same content of both sources'
        elif source_projectx_id and not rovi_projectx_id:
            comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
            status='Fail'
            initialization_file.rovi_id_not_ingested_count=initialization_file.rovi_id_not_ingested_count+1
        elif rovi_projectx_id and not source_projectx_id:
            comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
            status='Fail'
            initialization_file.ource_id_not_ingested_count=initialization_file.source_id_not_ingested_count+1
        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":rovi_projectx_id,
                                                                                  "projectx_id_%s"%source:'Nil',"Episode mapping":status,"Comment":comment})
    #TODO: condition 3, to check both source id ingested
    elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
        initialization_file.both_source_not_ingested_count=initialization_file.both_source_not_ingested_count+1
        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":'',"projectx_id_%s"%source:'',
                                                                "Episode mapping":'Fail',"Comment":'No Ingestion of both sources'})
    #TODO: condition 4, to check rovi_id ingested
    elif len(source_projectx_id)==1 and len(rovi_projectx_id)==0:
        initialization_file.rovi_id_not_ingested_count=initialization_file.rovi_id_not_ingested_count+1
        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":'',
                        "projectx_id_%s"%source:str(source_projectx_id[0]),"Episode mapping":'Fail',"Comment":'No Ingestion of rovi_id of episode'})
    #TODO: condition 5, to check source id ingested and checking for another source_ids   
    elif len(source_projectx_id)==0 and len(rovi_projectx_id)==1:
        retry_count=0
        try:
            duplicate_api=initialization_file.api_duplicate_checking_sm%(source_sm_id,source)
            data_resp_url=fetch_api_response(duplicate_api)
            if data_resp_url==[]:
                initialization_file.source_id_not_ingested_count=initialization_file.source_id_not_ingested_count+1 
                writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                         "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'})  
            else:
                for ll in data_resp_url:
                    if ll.get("projectxId") not in source_projectx_id_sm:
                        source_projectx_id_sm.append(ll.get("projectxId"))

                for sm_id in source_projectx_id_sm:       
                    projectx_url=initialization_file.projectx_preprod_api_episodes%sm_id
                    data_resp_url=fetch_api_response(projectx_url)
                    if data_resp_url!=[]:
                        for hh in data_resp_url:
                            source_projectx_id.append(hh.get("id"))            
                       
                if source_projectx_id!=[]:
                    if rovi_projectx_id[0] in source_projectx_id:
                        initialization_file.mapped_count=initialization_file.mapped_count+1
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,
                              "projectx_id_rovi":str(rovi_projectx_id),"projectx_id_%s"%source:str(rovi_projectx_id),"Episode mapping":'Pass',"Comment":'Pass'})
                    else:
                        projectx_mapping_api=initialization_file.projectx_mapping_api_domain%rovi_projectx_id[0]
                        data_mapped_resp=fetch_api_response(projectx_mapping_api)
                        another_source_id_present_status=to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source)
         
                        initialization_file.not_mapped_count=initialization_file.not_mapped_count+1
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,
                                        "projectx_id_rovi":str(rovi_projectx_id),"projectx_id_%s"%source:['not found match id'],
                                        "Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],
                                                                         "Episode mapping":'Fail',"Comment":'Fail'}) 
                else:
                    initialization_file.source_id_not_ingested_count=initialization_file.source_id_not_ingested_count+1 
                    writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                               "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'})                                                                
        except (httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,Exception) as e:
            retry_count+=1
            print ("exception caught ....................................................",type(e),thread_name,source_id.rovi_id)
            print("\n")
            print ("Retrying.............",retry_count)
            if retry_count<=5:
                checking_mapping_episodes(source_id,rovi_id,source_sm_id,rovi_series_id,writer,thread_name,total,source)
            else:
                retry_count=0    
      
    print("\n")
    print("thread thread_name:",thread_name,"multiple mapped: ",initialization_file.multiple_mapped_count,"mapped :",initialization_file.mapped_count ,"Not mapped :", initialization_file.not_mapped_count,
           "rovi id not ingested :", initialization_file.rovi_id_not_ingested_count, "source Id not ingested : ",initialization_file.source_id_not_ingested_count, "both not ingested: ",initialization_file.both_source_not_ingested_count)

    print(datetime.datetime.now())

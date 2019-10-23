"""Writer: Saayan"""

import sys
import urllib2
import json
import os
from urllib2 import URLError,HTTPError
import csv
import datetime
import httplib
import socket
import initialization_file
 
#TODO: to fetch response from API
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

        elif mapped.get("type")=="Program" and mapped.get("data_source")==source and mapped.get("sub_type")=='MO':
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

        elif mapped.get("type")=="Program" and mapped.get("data_source")==source and mapped.get("sub_type")=='MO':
            dev_source_px_mapped.append({"source_id":str(mapped.get("source_id"))})                   

    return {"another_rovi_id_present":another_rovi_id_present,"dev_source_px_mapped":dev_source_px_mapped}      

def to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id):
    variant_present='False'
    for kk in projectx_api_response:
        if kk.get("variant_parent_id") is not None:  
            px_variant_id.append(kk.get("variant_parent_id"))
    for id_ in px_array:
        if id_ in px_variant_id:
            variant_present='True'
            break
    return variant_present        

#TODO: checking mapping movies for the given sources(ROVI, GB)
def checking_mapping_movies(source_id,rovi_id,source_projectx_id,rovi_projectx_id,writer,thread_name,source):
    dev_rovi_px_mapped=[]
    dev_source_px_mapped=[]
    px_series_id=[]
    px_array=[]
    px_variant_id=[]
    #TODO: condition 1 , to check multiple projectx ids for a source
    if len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
        initialization_file.multiple_mapped_count+=1
        status='Nil'
        comment=''
        if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
            if source_projectx_id in rovi_projectx_id:
                status='Pass'
                comment='Multiple ingestion for same content of rovi'  
            else:
                comment= 'Multiple ingestion for same content of rovi' 
                status='Fail' 
        elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
            if rovi_projectx_id[0] in source_projectx_id:
                status='Pass'
                comment='Multiple ingestion for same content of %s'%source
            else:
                comment= 'Fail:Multiple ingestion for same content of %s'%source
                status='Fail'   
        elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
            for x in source_projectx_id:
                if x in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of both sources'
                    break;
                else:
                    comment= 'Fail:Multiple ingestion for same content of both sources'   
                    status='Fail'
        elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
            comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
            status='Fail'
        elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
            comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
            status='Fail'
        writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,
                           "projectx_id_%s"%source:source_projectx_id,"Comment":comment,"Result of mapping":status})    

    #TODO: condition 2, to check both projectx ids are same or not
    elif len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
        if rovi_projectx_id == source_projectx_id:
            initialization_file.mapped_count+=1
            writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                   "projectx_id_%s"%source:str(source_projectx_id[0]),"Comment":'Pass',"Result of mapping":'Pass'})

        else:
            initialization_file.not_mapped_count+=1
            #import pdb;pdb.set_trace()
            #TODO: to check the mapping of rovi_projectx_id from mapping API
            projectx_mapping_api=initialization_file.projectx_mapping_api_domain%rovi_projectx_id[0]
            data_mapped_resp=fetch_api_response(projectx_mapping_api)
            another_source_id_present_status=to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source)   
            #TODO: To check the mapping of source_projetx_id from mapping API
            projectx_mapping_api=initialization_file.projectx_mapping_api_domain%source_projectx_id[0]
            data_mapped_resp_source=fetch_api_response(projectx_mapping_api)
            another_rovi_id_present_status=to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source)
            px_array=[source_projectx_id[0],rovi_projectx_id[0]]
            #TODO: checking variant parent id for PX_ids
            projectx_api=initialization_file.projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
            projectx_api_response=fetch_api_response(projectx_api)
            variant_present=to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id)
                           
            writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                  "projectx_id_%s"%source:str(source_projectx_id[0]),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                 "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,
                                                     "Comment":'Fail',"Result of mapping":'Fail'})

    #TODO: condition 3, to check rovi_id ingested 
    elif len(source_projectx_id)==1 and not rovi_projectx_id:
        initialization_file.rovi_id_not_ingested_count+=1
        writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":'Nil',
                                                    "projectx_id_%s"%source:str(source_projectx_id),"Comment":'rovi_id not ingested',"Result of mapping":'Fail'})

    #TODO: condition 4, to check source id ingested and checking for another source_ids
    elif len(rovi_projectx_id)==1 and not source_projectx_id:
        try:
            retry_count=0
            #import pdb;pdb.set_trace()
            #TODO: to check duplicate id from source mapping API
            duplicate_api=initialization_file.api_duplicate_checking_mo%(eval(source_id),source)
            data_resp_url=fetch_api_response(duplicate_api)
            if data_resp_url==[]:
                initialization_file.source_id_not_ingested_count+=1
                writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id),
                                                                       "projectx_id_%s"%source:'Nil',"Comment":'source_id not ingested',"Result of mapping":'Fail'}) 
            else: 
                #TODO: taking source_px_id from duplicate API                        
                for ll in data_resp_url:
                    if ll.get("projectxId") not in source_projectx_id:
                        source_projectx_id.append(ll.get("projectxId"))
                if rovi_projectx_id[0] in source_projectx_id:
                    initialization_file.mapped_count+=1        
                    writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id),
                                                                            "projectx_id_%s"%source:str(source_projectx_id),"Comment":'Pass',"Result of mapping":'Pass'})   
                elif len(source_projectx_id)==1:
                    initialization_file.not_mapped_count+=1
                    #TODO: checking the mapping of rovi_projectx_id
                    projectx_mapping_api=initialization_file.projectx_mapping_api_domain%rovi_projectx_id[0]
                    data_mapped_resp=fetch_api_response(projectx_mapping_api)
                    another_source_id_present_status=to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source)
                    #TODO: checking the mapping of source_projectx_id
                    projectx_mapping_api=initialization_file.projectx_mapping_api_domain%source_projectx_id[0]
                    data_mapped_resp_source=fetch_api_response(projectx_mapping_api)
                    another_rovi_id_present_status=to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source)   
                    #TODO : to check variant parent ids for Px_ids
                    px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                    projectx_api=initialization_file.projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                    projectx_api_response=fetch_api_response(projectx_api)
                    variant_present=to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id) 

                    writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id),
                                    "projectx_id_%s"%source:str(source_projectx_id),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                    "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,
                                                               "Comment":'Fail',"Result of mapping":'Fail',})
                elif len(source_projectx_id)>1:  
                    initialization_file.multiple_mapped_count=initialization_file.multiple_mapped_count+1  
                    writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id),
                                       "projectx_id_%s"%source:'Nil',"Comment":'multiple source_px_id from duplicate API',"Result of mapping":'Fail'})    
                else:
                    initialization_file.source_id_not_ingested_count=initialization_file.source_id_not_ingested_count+1
                    writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id),
                                       "projectx_id_%s"%source:'Nil',"Comment":'Source_id not ingested',"Result of mapping":'Fail'})                  
                    
        except (httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError) as e:
            print ("exception caught ..................................................",type(e),thread_name,source_id,rovi_id)
            if retry_count<=5:
                checking_mapping_movies(source_id,rovi_id,source_projectx_id,rovi_projectx_id,total,writer,thread_name,source)
            else:
               retry_count=0

    #TODO: condition 5, to check both source id ingested
    elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
        initialization_file.both_source_not_ingested_count+=1
        writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_%s"%source:'',
                                        "Comment":'both ids not ingested',"Result of mapping":'N.A'})
    
    print("\n")
    print("thread_name:",thread_name,"multiple mapped: ",initialization_file.multiple_mapped_count ,"mapped :",initialization_file.mapped_count ,"Not mapped :", initialization_file.not_mapped_count,
          "rovi id not ingested :", initialization_file.rovi_id_not_ingested_count, "source Id not ingested : ",initialization_file.source_id_not_ingested_count, "both not ingested: ",initialization_file.both_source_not_ingested_count)

    print(datetime.datetime.now())
import os
import sys
import urllib2
import json
import httplib
import socket
sys.path.insert(0,os.getcwd()+'/common_lib')
sys.path.insert(1,os.getcwd()+'/common_lib/Px_lib')
sys.path.insert(2,os.getcwd()+'/common_lib/source_lib')
sys.path.insert(3,os.getcwd()+'/common_lib/validation_lib')
import initialization_file

#TODO: To fetch response from API 
def fetch_from_cloud_for_id(api):
    resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':initialization_file.token}))
    data = resp.read()
    data_resp = json.loads(data)
    return data_resp

#TODO: to get px_id from mapping API
def getting_mapped_px_id(_id,show_type,source):
    try:
        retry_count=0
        px_id=[]
        source_map=[]
        any_source_flag='False'
        source_flag='False'
        #import pdb;pdb.set_trace()
        source_mapping_api=initialization_file.source_mapping_api%(_id,source,show_type)
        data_resp_mapping=fetch_from_cloud_for_id(source_mapping_api)

        for data in data_resp_mapping:
            if data.get("data_source")==source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                px_id.append(data.get("projectxId"))

        if px_id:       
            #import pdb;pdb.set_trace() 
            px_mapping_api=initialization_file.projectx_mapping_api%px_id[0]
            data_resp_px_mapping=fetch_from_cloud_for_id(px_mapping_api)

            for resp in data_resp_px_mapping:
                if (resp.get("data_source")=='Rovi' or resp.get("data_source")=='GuideBox' or resp.get("data_source")=='Hulu' or resp.get("data_source")=='Vudu') and resp.get("type")=='Program':
                    source_map.append({str(resp.get("data_source")):resp.get("sourceId")})
                    any_source_flag='True'
                    break
                elif resp.get("data_source")==source and resp.get("type")=='Program' and resp.get("sub_type")==show_type:
                    source_map.append({str(resp.get("data_source")):resp.get("sourceId")})
                    source_flag='True'
            if  source_flag=='True' and any_source_flag=='False':
                return (px_id[0],_id,source_map)
                
    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
        retry_count+=1
        print ("exception caught getting_mapped_px_id ............................................",type(e),_id,source,show_type)
        print ("\n") 
        print ("Retrying.............",retry_count)
        print ("\n")    
        if retry_count<=5:
            getting_mapped_px_id(_id,show_type,source)            
        else: 
            retry_count=0   
    
                       
                    


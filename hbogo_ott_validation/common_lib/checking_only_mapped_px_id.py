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

#TODO: to get mapped PX_id from the mapping API  
def getting_mapped_px_id(_id,show_type,source):
    try:
        px_id=[]
        source_map=[]
        any_source_flag='False'
        source_flag='False'
        retry_count=0

        #import pdb;pdb.set_trace()
        source_mapping_api=initialization_file.source_mapping_api%(_id,source,show_type)
        data_resp_mapping=fetch_from_cloud_for_id(source_mapping_api)

        for data in data_resp_mapping:
            if data.get("data_source")==source and data.get("type")=='Program' and data.get("sub_type")==show_type:
                px_id.append(data.get("projectxId"))
        print("\n")        
        print ({"px_id":px_id})
        if px_id:       
            #import pdb;pdb.set_trace() 
            px_mapping_api=initialization_file.projectx_mapping_api%px_id[0]
            data_resp_px_mapping=fetch_from_cloud_for_id(px_mapping_api)

            for resp in data_resp_px_mapping:
                if (resp.get("data_source")=='Rovi' or resp.get("data_source")=='GuideBox' or resp.get("data_source")=='Hulu' or resp.get("data_source")=='Vudu') and resp.get("type")=='Program':
                    source_map.append({str(resp.get("data_source")):resp.get("sourceId")})
                    any_source_flag='True(Rovi+others)'
                elif resp.get("data_source")==source and resp.get("type")=='Program' and resp.get("sub_type")==show_type:
                    source_map.append({str(resp.get("data_source")):resp.get("sourceId")})
                    source_flag='True'

            # separate PX_ids with flag which is only mapped to source and mapped to others        
            if  source_flag=='True' and any_source_flag=='False':
                return {"px_id":px_id[0],"source_id":_id,"source_flag":source_flag,"source_map":source_map}
            elif source_flag=='False' and any_source_flag=='True(Rovi+others)':    
                return {"px_id":px_id[0],"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
            elif source_flag=='True' and any_source_flag=='True(Rovi+others)':
                return {"px_id":px_id[0],"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
        else:   
            return {"px_id":px_id,"source_id":_id,"source_flag":any_source_flag,"source_map":source_map}
                    

    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
        #import pdb;pdb.set_trace()
        retry_count+=1
        if retry_count<=5: 
            print ("exception caught in getting_mapped_px_id func....................................",type(e),_id,source,show_type)
            print ("\n") 
            print ("Retrying.............",retry_count)
            print ("\n")    
            getting_mapped_px_id(_id,show_type,source)
        else:
            retry_count=0                
    
                       
                    


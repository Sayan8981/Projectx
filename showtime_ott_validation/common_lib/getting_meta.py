"""Writer: Saayan"""

import sys
import csv
import datetime
import urllib2
import json
import os
from urllib2 import HTTPError
import httplib
import socket
import pinyin
import unidecode
sys.path.insert(0,os.getcwd()+'/common_lib')
sys.path.insert(3,os.getcwd()+'/common_lib/validation_lib')
import initialization_file
  

#TODO: to get source_detail OOT link id only from DB table
def getting_source_details(showtime_id,show_type,source,details,id_):
    #import pdb;pdb.set_trace()
    showtime_link_present='Null'
    showtime_id=(details[id_])[0]
    showtime_link=(details[id_])[4]
    if showtime_link!="" or showtime_link is not None :
        showtime_link_present='True' 

    return {"source_link_present":showtime_link_present,"source_id":showtime_id,"show_type":show_type}

#TODO: to get projectx_id details OTT link id from programs api
def getting_projectx_details(projectx_id,show_type,source):
  
    px_video_link=[]
    px_video_link_present='False'
    px_response='Null'
    launch_id=[]
    #import pdb;pdb.set_trace()

    projectx_api=initialization_file.projectx_preprod_domain%projectx_id
    px_resp=urllib2.urlopen(urllib2.Request(projectx_api,None,{'Authorization':initialization_file.token}))
    data_px=px_resp.read()
    data_px_resp=json.loads(data_px)
    if data_px_resp!=[]:
        for data in data_px_resp:
            px_video_link= data.get("videos")
            if px_video_link:
                px_video_link_present='True'
                for linkid in px_video_link:
                    #if linkid.get("source_id")=='showtime':
                    launch_id.append(linkid.get("launch_id"))
        return {"px_video_link_present":px_video_link_present,"launch_id":launch_id}
    else:
        return px_response
    
        


         


            



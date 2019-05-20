"""Checking Episode sequence for only mapped Guidebox programs in projectx Episodes """

"""Writer: Saayan"""


import unidecode
import pymongo
from pprint import pprint
import sys
import csv
import collections
from collections import Counter
import datetime
import urllib2
import json
import os
from urllib2 import HTTPError
import urllib
import socket
import httplib
from urllib2 import URLError
sys.setrecursionlimit(20000)
import unidecode
import threading
import re
import unidecode
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


def get_result_episode_sequence(Px_sm_id,Gb_sm_id,show_type,name,writer,total):
    
    token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
    mapped_gb_id=[]
    series_id_match=''
    episode_number_match=''
    season_number_match=''
    comment=''
    gb_episode_title=''
    gb_show_type=''
    show_id=0
    gb_episode_number=0
    gb_season_number=0
    gb_series_title=''
    gb_id=0
    show_id=''
    gb_show_type=''
    px_episode_id=0
    px_show_type=''
    px_series_id=0
    px_series_title=''
    px_episode_title=''
    
    px_episode_number=0
    px_season_number=0


    Total=0
#    import pdb;pdb.set_trace()
    try:
        GB_details_api_SM="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=SM"%eval(Gb_sm_id)
        GB_link_sm=urllib2.Request(GB_details_api_SM)
        GB_link_sm.add_header('Authorization',token)
        GB_resp_sm=urllib2.urlopen(GB_link_sm)
        data_GB_sm=GB_resp_sm.read()
        data_GB_resp_sm=json.loads(data_GB_sm)
        gb_series_title=unidecode.unidecode(data_GB_resp_sm.get("title"))

    	
        episode_api="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d/episodes?&ott=true"%eval(Px_sm_id)

        episode_link=urllib2.Request(episode_api)
        episode_link.add_header('Authorization',token)
        resp_link=urllib2.urlopen(episode_link)
        data_link=resp_link.read()
        data_resp_link=json.loads(data_link)
        for kk in data_resp_link:

            mapped_gb_id=[]
            Total=Total+1
            print ("\n")    
            print (kk.get("id"),kk.get("show_type"),kk.get("series_id"),kk.get("original_episode_title"),Total,name)
            px_episode_id=kk.get("id")
            px_show_type=kk.get("show_type").encode('utf-8')
            px_series_id=kk.get("series_id")
            px_series_title=unidecode.unidecode(kk.get("long_title"))
            px_episode_title=unidecode.unidecode(kk.get("original_episode_title"))
            if px_episode_title=='':
                px_episode_title=unidecode.unidecode(kk.get("episode_title"))
            px_episode_number=kk.get("episode_season_sequence")
            px_season_number=kk.get("episode_season_number")
            mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%px_episode_id
            mapping_link=urllib2.Request(mapping_api)
            mapping_link.add_header('Authorization',token)
            mapped_resp=urllib2.urlopen(mapping_link)
            data_mapped=mapped_resp.read()
            data_mapped_resp=json.loads(data_mapped)
            for ll in data_mapped_resp:
            	
                if ll.get("data_source")=='GuideBox' and ll.get("sub_type")=='SE' and ll.get("map_reason")=='new' and ll.get("type")=='Program':
                    mapped_gb_id.append(ll.get("source_id").encode('utf-8'))
            if len(mapped_gb_id)==1:
                GB_details_api="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=SE"%eval(mapped_gb_id[0])
                GB_link=urllib2.Request(GB_details_api)
                GB_link.add_header('Authorization',token)
                GB_resp=urllib2.urlopen(GB_link)
                data_GB=GB_resp.read()
                data_GB_resp=json.loads(data_GB)
	            
                gb_id=data_GB_resp.get("gb_id")
                show_id=data_GB_resp.get("show_id")
                gb_show_type=data_GB_resp.get("show_type")
                gb_episode_number=data_GB_resp.get("episode_number")
                gb_episode_title=unidecode.unidecode(data_GB_resp.get("original_title"))
                if gb_episode_title=='':
                    gb_episode_title=unidecode.unidecode(data_GB_resp.get("title"))
                gb_season_number=data_GB_resp.get("season_number")

                

                
            elif len(mapped_gb_id)==0:
                comment ='Rovi id mapped for this episode'
            else:
                comment ='multiple Gb_id mapped for this episode'
            
    
            if len(mapped_gb_id)==1: 
	            if str(px_series_id)==Px_sm_id:
	                series_id_match='True'
	            else:
	                series_id_match='False'

	            if px_episode_number==gb_episode_number:
	                episode_number_match='True'
	            else:
	                episode_number_match='False'

	            if px_season_number==gb_season_number:
	                season_number_match='True'                	
	            else:
	                season_number_match='False'    

            writer.writerow([show_id,gb_id,gb_show_type,gb_series_title,gb_episode_title,gb_season_number,gb_episode_number,px_series_id,px_episode_id,px_show_type,px_series_title,px_episode_title,px_season_number,px_episode_number,series_id_match,episode_number_match,season_number_match,comment])
            print datetime.datetime.now()  

    except httplib.BadStatusLine:
        print ("exception caught httplib.BadStatusLine................................................................................",name)
        print ("\n") 
        print ("Retrying.............")
        print ("\n") 
    	get_result_episode_sequence(Px_sm_id,Gb_sm_id,show_type,name,writer,total)
    except urllib2.HTTPError:
        print ("exception caught HTTPError................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        get_result_episode_sequence(Px_sm_id,Gb_sm_id,show_type,name,writer,total)
    except socket.error:
        print ("exception caught SocketError..........................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        get_result_episode_sequence(Px_sm_id,Gb_sm_id,show_type,name,writer,total)   
    except URLError:
        print ("exception caught URLError.............................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        get_result_episode_sequence(Px_sm_id,Gb_sm_id,show_type,name,writer,total)
    except RuntimeError:
        print ("exception caught ..................................................................................",name,)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        get_result_episode_sequence(Px_sm_id,Gb_sm_id,show_type,name,writer,total) 
    except ValueError:
        print ("exception caught ..................................................................................",name,)
        print ("\n")
        print ("neglecting this id.............",kk.get("id"),kk.get("show_type"),kk.get("series_id"))
        pass    

def episode_sequence(start,name,end,id):

    v=''
    inputFile="gb_all_series_projectx_maps"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/episode_sequence_checked_in_projectx_GB_mapped%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    v=open(os.getcwd()+result_sheet,"wa")
    total=0
    
    with v as mycsvfile:
        fieldnames = ["GB_show_id","gb_id","gb_show_type","gb_series_title","gb_episode_title","gb_season_number","gb_episode_number","px_series_id","px_episode_id","px_show_type","px_series_title","px_episode_title","px_season_number","px_episode_number","series_id_match","episode_number_match","season_number_match","Comment"]

        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        for aa in range(start,end):
            total=total+1
            #import pdb;pdb.set_trace()
            Px_sm_id=fullist[aa][0]
            Gb_sm_id=fullist[aa][1]
            show_type=fullist[aa][2]
            print ("\n")
            print (Px_sm_id,Gb_sm_id,show_type,name,writer,total)
            if show_type=='SM':  
                get_result_episode_sequence(Px_sm_id,Gb_sm_id,show_type,name,writer,total)


t1=threading.Thread(target=episode_sequence,args=(1,"thread - 1",15001,1))
t1.start()
t1=threading.Thread(target=episode_sequence,args=(15001,"thread - 2",30001,2))
t1.start()
t1=threading.Thread(target=episode_sequence,args=(30001,"thread - 3",45001,3))
t1.start()
t1=threading.Thread(target=episode_sequence,args=(45001,"thread - 4",61466,4))
t1.start()

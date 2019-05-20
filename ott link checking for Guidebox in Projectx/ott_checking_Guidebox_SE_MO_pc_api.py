
"""writer : Saayan """

import threading
import pymongo
from pprint import pprint
import sys
import os
import csv
import pymysql
import collections
from pprint import pprint
import MySQLdb
import re
import collections
from collections import Counter
import datetime
import _mysql_exceptions
import urllib2
import json
from urllib2 import HTTPError
from urllib2 import URLError
import csv
import os
import socket
import httplib


def ott(start,name,end,id):
    print name
    print("Checking Ott_contents of SE for Guidebox to Projectx for pc platform only ........")

    connection=pymongo.MongoClient("mongodb://192.168.86.10:27017/")
    mydb=connection["ozone"]
    mytable=mydb["guidebox_program_details"]
     
    result_sheet1='/validation_ott_links_content_gb_SE_duplicate%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet1)):
        os.remove(os.getcwd()+result_sheet1)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    v=open(os.getcwd()+result_sheet1,"wa")

    result_sheet='/validation_ott_links_content_gb_SE%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile,v as mycsvfile1:
        fieldnames = ["Gb_sm_id","Gb_id","show_type","projectx_id_gb","link_mismatch","Wrong link","purchase_source_program_id_mismatch","Wrong_source_program_id","format_mismatch","Wrong format","price_mismatch","Wrong price","purchase_type_mismatch","wrong purchase type","purchase_launch_id_mismatch","wrong purchase_launch_id","purchase_roku_id_mismatch","wrong purchase_roku_id","purchase_firetv_id_mismatch","wrong purchase_firetv_id","purchase_appletv_id_mismatch","wrong purchase_appletv_id","purchase_androidtv_id_mismatch","wrong purchase_androidtv_id","subscription_link_mismatch","Wrong subscription_link","tv_everywhere_link_mismatch","Wrong tv_everywhere_link","tv_everywhere_source_program_id_mismatch","Wrong tv_everywhere_source_program_id","tv_everywhere_launch_id_mismatch","Wrong tv_everywhere_launch_id","tv_everywhere_roku_id_mismatch","Wrong tv_everywhere_roku_id","tv_everywhere_firetv_id_mismatch","Wrong tv_everywhere_firetv_id","tv_everywhere_appletv_id_mismatch","Wrong tv_everywhere_appletv_id","tv_everywhere_androidtv_id_mismatch","Wrong tv_everywhere_androidtv_id","subscription_source_program_id_mismatch","Wrong subscription_source_program_id","subscription_launch_id_mismatch","wrong subscription_launch_id","subscription_roku_id_mismatch","wrong subscription_roku_id","subscription_firetv_id_mismatch","wrong subscription_firetv_id","subscription_appletv_id_mismatch","wrong subscription_appletv_id","subscription_androidtv_id_mismatch","wrong subscription_androidtv_id","Result for Ott_link population"]


        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer1 = csv.DictWriter(mycsvfile1,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        writer1.writeheader()
        total=0
        total1=0
        t=0
        v=0
        m=0
        w=0
        h=0 
        ss=0 
        GB_id=[]
        GB_Id=[] 
        Gb_id=[]
        Gb_sm_id=[]
        for aa in range(start,end,1000):
      	    try:
                res1=mytable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"$and":[{"show_type":{"$in":["SE","MO"]}}]}},{"$project":{"gb_id":1,"show_id":1,"_id":0,"show_type":1}}])
                print "start_id: "+str(start)
                for i in res1:
                    print("\n") 
                    print("gb_ids not ingested",GB_id, len(GB_id))
                    gb_id=i.get("gb_id")
                    print("\n") 
                    print "start_id: "+str(start)
                    print("\n")
    	            print "end_id: "+str(end)
                    gb_sm_id=i.get("show_id")
                    show_type=i.get("show_type")
                    print("\n")
                    print gb_id 
                    gb_projectx_id_sm=[]
                    if show_type=="SE":
                        mapping_url="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=SE"%gb_id   
                        response=urllib2.Request(mapping_url)
                        response.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp=urllib2.urlopen(response)
                        data=resp.read()
                        data_resp=json.loads(data)
                    else:
                        mapping_url="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=MO"%gb_id
                        response=urllib2.Request(mapping_url)
                        response.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp=urllib2.urlopen(response)
                        data=resp.read()
                        data_resp=json.loads(data)
                    if data_resp==[]:
                        if show_type=="SE":

                            duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=SM"%gb_sm_id
                            response_url=urllib2.Request(duplicate_api)
                            response_url.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_url=urllib2.urlopen(response_url)
                            data_url=resp_url.read()
                            data_resp_url=json.loads(data_url)
                        else:
                            duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=MO"%gb_id
                            response_url=urllib2.Request(duplicate_api)
                            response_url.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_url=urllib2.urlopen(response_url)
                            data_url=resp_url.read()
                            data_resp_url=json.loads(data_url)
                        if data_resp_url==[]:
                            if show_type=="SE":
                                id_api="http://34.231.212.186:81/projectx/guidebox?sourceId=%d&showType=SM"%gb_sm_id
                                response_url1=urllib2.Request(id_api)
                                response_url1.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_url1=urllib2.urlopen(response_url1)
                                data_url1=resp_url1.read()
                                data_resp_url1=json.loads(data_url1)
                            else:
                                id_api="http://34.231.212.186:81/projectx/guidebox?sourceId=%d&showType=MO"%gb_id
                                response_url1=urllib2.Request(id_api)
                                response_url1.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_url1=urllib2.urlopen(response_url1)
                                data_url1=resp_url1.read()
                                data_resp_url1=json.loads(data_url1)

                            if data_resp_url1==True:
                                if show_type=="SE":
                                    w=w+1
                                    if [{str(gb_id):str(gb_sm_id)}] not in GB_id:
                                        GB_id.append([{str(gb_id):str(gb_sm_id)}])
                                        print("\n")
                                        print("gb_ids SE not ingested",GB_id, len(GB_id))
                                else:
                                    w=w+1
                                    if str(gb_id) not in GB_Id:
                                        GB_Id.append(str(gb_id))
                                        print("\n")
                                        print("gb_ids MO not ingested",GB_Id, len(GB_Id))   
                            else:
                                if show_type=="SE":
                                    ss=ss+1
                                    if [str(gb_sm_id)] not in Gb_sm_id:
                                        Gb_sm_id.append([str(gb_sm_id)])
                                        print("\n")
                                        print("gb_id_sm not in DB",Gb_sm_id, len(Gb_sm_id))
                                else:
                                    ss=ss+1
                                    if [str(gb_id)] not in GB_Id:
                                        GB_Id.append([str(gb_id)])
                                        print("\n")
                                        print("gb_id_MO not in DB",GB_Id, len(GB_Id))  
                        else:
                             while len(data_resp_url)>1:
                                 for i in data_resp_url:
                                     if data_resp_url.count(i)>1:
                                         data_resp_url.remove(i)
                             if len(data_resp_url)==1:
                                 if show_type=="SE":
                                     for ll in data_resp_url:
                                         gb_projectx_id_sm.append(ll.get("projectx_id"))    
                                     res2=mytable.find({"show_type":"SE","show_id":gb_projectx_id_sm[0]},{"gb_id":1,"show_id":1,"_id":0}) 
                                 
                                   
                                     for i in res2:
                                         gb_id=i.get("gb_id")
                                         gb_sm_id=i.get("show_id")
                                         print gb_id
    
                                     mapping_url="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=SE"%gb_id
                                     response=urllib2.Request(mapping_url)
                                     response.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                     resp=urllib2.urlopen(response)
                                     data=resp.read()
                                     data_resp=json.loads(data)
                                 else:
                                     gb_id=gb_id
                                     print gb_id
                                     mapping_url="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=MO"%gb_id
                                     response=urllib2.Request(mapping_url)
                                     response.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                     resp=urllib2.urlopen(response)
                                     data=resp.read()
                                     data_resp=json.loads(data)
                                 if data_resp==[]:
                                     if show_type=='SE':
                                         w=w+1
                                         GB_id.append([{str(gb_id):str(gb_sm_id)}])
                                         print("\n")
                                         print("gb_ids not ingested",GB_id, len(GB_id))  
                                     else:
                                         w=w+1
                                         GB_id.append(str(gb_id))
                                         print("\n")
                                         print("gb_ids MO not ingested",GB_Id, len(GB_Id))
                                 else:
                                     total1=total1+1
                                     for x in data_resp:
                                         projectx_id_gb=str(x["projectx_id"])
                                     price=''
                                     Px_ott_url_purchase=[]
                                     purchase_source_program_id=''
                                     Gb_ott_url_purchase=[]
                                     purchase_source_program_id_match=''
                                     purchase_source_program_id_match_=[]
                                     purchase_source_program_id_mismatch =''
                                     purchase_source_program_id_mismatch_ =[]
                                     purchase_launch_id_match=''
                                     purchase_launch_id_match_=[]
                                     purchase_launch_id_mismatch=''
                                     purchase_launch_id_mismatch_=[]
                                     purchase_roku_id_match=''
                                     purchase_roku_id_match_=[]
                                     purchase_roku_id_mismatch=''
                                     purchase_roku_id_mismatch_=[]
                                     purchase_firetv_id_match=''
                                     purchase_firetv_id_match_=[]
                                     purchase_firetv_id_mismatch_=[]
                                     purchase_firetv_id_mismatch=''
                                     purchase_appletv_id_match=''
                                     purchase_appletv_id_match_=[]
                                     purchase_appletv_id_mismatch=''
                                     purchase_appletv_id_mismatch_=[]
                                     purchase_androidtv_id_match=''
                                     purchase_androidtv_id_match_=[]
                                     purchase_androidtv_id_mismatch =''
                                     purchase_androidtv_id_mismatch_=[] 
                                     projectx_videos=''
                                     format_match=''
                                     format_match_=[]
                                     format_mismatch=''
                                     format_mismatch_=[]
                                     price_match=''
                                     price_match_=[]
                                     price_mismatch=''
                                     price_mismatch_ =[]
                                     purchase_type_match=''
                                     purchase_type_match_=[]
                                     purchase_type_mismatch=''
                                     purchase_type_mismatch_=[]
                                     projectx_videos_link_flag=''
                                     link_match=''
                                     link_mismatch=''
                                     link_mismatch_=[]
                                     subscription_source_program_id=''
                                     subscription_link_mismatch_=[]
                                     subscription_link_match=''
                                     subscription_link_mismatch=''
                                     subscription_source_program_id_match=''
                                     subscription_source_program_id_mismatch=''
                                     subscription_source_program_id_mismatch_=[]
                                     subscription_launch_id_match=''
                                     subscription_launch_id_mismatch =''
                                     subscription_launch_id_mismatch_=[]
                                     subscription_roku_id_match=''
                                     subscription_roku_id_mismatch =''
                                     subscription_roku_id_mismatch_=[]
                                     subscription_firetv_id_match=''
                                     subscription_firetv_id_mismatch=''
                                     subscription_firetv_id_mismatch_=[]
                                     subscription_appletv_id_match=''
                                     subscription_appletv_id_mismatch=''
                                     subscription_appletv_id_mismatch_=[]
                                     subscription_androidtv_id_match=''
                                     subscription_androidtv_id_mismatch=''
                                     subscription_androidtv_id_mismatch_=[]
                                     tv_everywhere_source_program_id=''
                                     tv_everywhere_link_match=''
                                     tv_everywhere_link_mismatch_=[]
                                     tv_everywhere_link_mismatch=''
                                     tv_everywhere_source_program_id_mismatch=''
                                     tv_everywhere_source_program_id_mismatch_=[]
                                     tv_everywhere_source_program_id_match=''
                                     tv_everywhere_launch_id_match=''
                                     tv_everywhere_launch_id_mismatch  =''
                                     tv_everywhere_launch_id_mismatch_=[]
                                     tv_everywhere_roku_id_match=''
                                     tv_everywhere_roku_id_mismatch=''
                                     tv_everywhere_roku_id_mismatch_=[]
                                     tv_everywhere_firetv_id_match=''
                                     tv_everywhere_firetv_id_mismatch=''
                                     tv_everywhere_firetv_id_mismatch_=[]
                                     tv_everywhere_appletv_id_match =''
                                     tv_everywhere_appletv_id_mismatch=''
                                     tv_everywhere_appletv_id_mismatch_=[]
                                     tv_everywhere_androidtv_id_match=''
                                     tv_everywhere_androidtv_id_mismatch=''
                                     tv_everywhere_androidtv_id_mismatch_=[]
                                     print("\n")
                                     print("gb_id : ", gb_id)
                                     print("gb_sm_id: ", gb_sm_id) 
                                     j=0
                                     if show_trpe=='SE':
                                         Guidebox_dump_api="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=SE"%gb_id
                                         response_GB_api=urllib2.Request(Guidebox_dump_api)
                                         response_GB_api.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                         resp=urllib2.urlopen(response_GB_api)
                                         data_GB=resp.read()
                                         data_resp_gb=json.loads(data_GB)
                                         projectx_url="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%eval(projectx_id_gb)
                                         response1=urllib2.Request(projectx_url)
                                         response1.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                         resp1=urllib2.urlopen(response1)
                                         data1=resp1.read()
                                         data_resp1=json.loads(data1)
                                     else:
                                         Guidebox_dump_api="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=MO"%gb_id
                                         response_GB_api=urllib2.Request(Guidebox_dump_api)
                                         response_GB_api.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                         resp=urllib2.urlopen(response_GB_api)
                                         data_GB=resp.read()
                                         data_resp_gb=json.loads(data_GB)
                                         projectx_url="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%eval(projectx_id_gb)
                                         response1=urllib2.Request(projectx_url)
                                         response1.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                         resp1=urllib2.urlopen(response1)
                                         data1=resp1.read()
                                         data_resp1=json.loads(data1) 
                                     for bb in data_resp1:
                                         projectx_videos=bb.get("videos")
                                     dict7_=dict()
                                     k=0
                                     g=0
                                     if data_resp_gb.get("purchase_web_sources")!=[]:
                                         #import pdb;pdb.set_trace()
                                         a=data_resp_gb.get("purchase_web_sources")
                                         if a!=[]:
                                             for x in data_resp1:
                                                 if x.get("videos"):
                                                     projectx_videos_link_flag=='True'
                                                     for mm in x.get("videos"):
                                                         if mm.get("fetched_from")=='GuideBox' and mm.get("constraints") is None and mm.get("constraints")!=[]:
                                                             Px_ott_url_purchase.append(mm.get("link").get("uri").encode("utf-8"))
                                                 else:
                                                     projectx_videos_link_flag=='True' 
                                         for nn in a :
                                             Gb_ott_url_purchase.append(nn.get("link"))

                                         if Px_ott_url_purchase:
                                             for jj in Px_ott_url_purchase:
                                                 if jj in Gb_ott_url_purchase:
                                                     link_match='True'
                                                     for mm in a:
                                                         for hh in projectx_videos:
                                                             if hh.get("fetched_from")=='GuideBox' and hh.get("link").get("uri").encode()==mm.get("link") and hh.get("constraints") is None and hh.get("constraints")!=[]:
                                                                 if 'vuduapp' in mm.get("link"):
                                                                     purchase_source_program_id=re.findall("\w+.*?", mm.get("link"))[-1:][0]
                                                                 if 'hbo.hbonow' in mm.get("link") or 'hbonow://asset' in mm.get("link"):
                                                                     purchase_source_program_id=re.findall("\w+.*?", mm.get("link"))[-1:][0]
                                                                 try:
                                                                     if 'play.google' in mm.get("link"):
                                                                         purchase_source_program_id=re.findall("\w+-.*?\-\-\w+.*?\w+\-(\w+.*)", mm.get("link"))[0]
                                                                 except IndexError:
                                                                     try:
                                                                         purchase_source_program_id=re.findall("\w+.*?\-(\-\w+.*?\w+.*?\w.*?\w)", mm.get("link"))[0]
                                                                     except IndexError:
                                                                         try:
                                                                             purchase_source_program_id=re.findall("\w+.*?\-(\w+.*?\w+)", mm.get("link"))[-1:][0]
                                                                         except IndexError:
                                                                             try:
                                                                                 purcahse_source_program_id=re.findall("\w+-.*?\-(\-\w+.*?)", mm.get("link"))[-1:][0]
                                                                             except IndexError:
                                                                                 try:
                                                                                     purchase_source_program_id=re.findall("\w+.*?\-(\w+.*?\w+\w+.\w.-*?\d*\w?)", mm.get("link"))[-1][0]
                                                                                 except IndexError:
                                                                                     try:
                                                                                         purchase_source_program_id=re.findall("\w+.*?\-(\w+.*?\w+.-*\w.*?\w+)", mm.get("link"))[-1:][0]  #### 
                                                                                     except IndexError:     
                                                                                         purchase_source_program_id=re.findall("w+.*?",mm.get("link"))[-1:][0]

                                                                 try:
                                                                     if '//itunes.apple.com/us/tv-season' in mm.get("link"):
                                                                         purchase_source_program_id=re.findall("\d+", mm.get("link"))[-2:-1][0]
                                                                 except IndexError:
                                                                     try:
                                                                         purchase_source_program_id=re.findall("\d+", mm.get("link"))[1:-2][0]
                                                                     except IndexError:
                                                                         purchase_source_program_id=re.findall("\d+", mm.get("link"))[0]
                                                                 try:
                                                                     if '//itunes.apple.com/us/movie' in mm.get("link"):
                                                                         purchase_source_program_id=re.findall("\d+", b)[0:-2][2:][1]
                                                                 except IndexError:
                                                                     try:
                                                                         purchase_source_program_id=re.findall("\d+", mm.get("link"))[1:-2][1]
                                                                     except IndexError:
                                                                         try:
                                                                             purchase_source_program_id=re.findall("\d+", mm.get("link"))[0:-2][1:2][0]
                                                                         except IndexError:
                                                                             purchase_source_program_id=re.findall("\d+", mm.get("link"))[0]
                                                                 if '//www.amazon.com/gp' in mm.get("link"):
                                                                     purchase_source_program_id=re.findall("\w+\d+\w+", mm.get("link"))[0]
                                                                 if '//click.linksynergy.com/' in mm.get("link"):
                                                                     purchase_source_program_id=re.findall("\d+.*?", mm.get("link"))[-1:][0]
                                                                 try:
                                                                     if '//www.youtube.com/w' in mm.get("link"):
                                                                         purchase_source_program_id=re.findall("\w+-.*?\w+-\w+",mm.get("link"))[0]
                                                                 except IndexError:
                                                                     try:
                                                                         purchase_source_program_id=re.findall("\w+-\w+.*?",mm.get("link"))[0]
                                                                     except IndexError:
                                                                         try:
                                                                             purchase_source_program_id=re.findall("\-\w+.*?",mm.get("link"))[0]
                                                                         except IndexError:
                                                                             try:
                                                                                 purchase_source_program_id=re.findall("\w+.*?\w+", mm.get("link"))[-1:][0]
                                                                             except IndexError:
                                                                                 try:
                                                                                     purchase_source_program_id=re.findall("\w+\d+\w+", mm.get("link"))[0]
                                                                                 except IndexError:
                                                                                     purchase_source_program_id=re.findall("\w+.*?",mm.get("link"))[-1:][0]
                                                                 if '//www.verizon.com/' in mm.get("link"):
                                                                     try:
                                                                         purchase_source_program_id=re.findall("\w+\d+", mm.get("link"))[-1:][0]   #"""***************"""
                                                                     except IndexError:
                                                                         purchase_source_program_id=re.findall("\d+", mm.get("link"))[-1]
                                                                 if 'https://www.paramountmovies.com' in mm.get("link"):
                                                                     try:
                                                                         purcahse_source_program_id=re.findall("\w+\d+" ,mm.get("link"))[-1:][0]
                                                                     except IndexError:
                                                                         try:
                                                                             purchase_source_program_id=re.findall("\w+\d+\w+" ,mm.get("link"))[-1:][0]
                                                                         except IndexError:
                                                                             try:
                                                                                 purchase_source_program_id=re.findall("\w+" ,mm.get("link"))[-1:][0]
                                                                             except IndexError:
                                                                                 pass
                                                                 if '//www.verizon.com/Ondemand/Movies/' in mm.get("link"):
                                                                     purchase_source_program_id=re.findall("\w+\d+\w+", b)[0]
                                                                 if 'https://store.sonyentertainmentnetwork.com/' in mm.get("link"):
                                                                     purchase_source_program_id=re.findall("\w+-\w+-\w+.*?",mm.get("link"))[0]
                                                                 if 'https://www.mgo.com/' in mm.get("link"):
                                                                     purchase_source_program_id=re.findall("\w+\d+\w+",mm.get("link"))[0]
                                          #                       import pdb;pdb.set_trace()
                                                                 if hh.get("source_program_id") is not None: 
                                                                     if hh.get("source_program_id").encode('utf-8')==purchase_source_program_id:
                                                                         purchase_source_program_id_match='True'
                                                                         purchase_source_program_id_match_=['True']
                                                                     else:
                                                                         purchase_source_program_id_mismatch='True'
                                                                         purchase_source_program_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", purchase_source_program_id])
                                                                 if hh.get("launch_id") is not None:
                                                                     if hh.get("launch_id").encode('utf-8')==purchase_source_program_id:
                                                                         purchase_launch_id_match="True"
                                                                         purchase_launch_id_match_=['True']
                                                                     else:
                                                                         purchase_launch_id_mismatch='True'
                                                                         purchase_launch_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("launch_id")])

                                                                 if hh.get("roku_id") is not None:
                                                                     if purchase_source_program_id in hh.get("roku_id").encode('utf-8'):
                                                                         purchase_roku_id_match="True"
                                                                         purchase_roku_id_match_=['True']
                                                                     else:
                                                                         purchase_roku_id_mismatch='True'
                                                                         purchase_roku_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("roku_id")])

                                                                 if hh.get("firetv_id") is not None:
                                                                     if purchase_source_program_id in hh.get("firetv_id").encode('utf-8'):
                                                                         purchase_firetv_id_match="True"
                                                                         purchase_firetv_id_match_=['True']
                                                                     else:
                                                                         purchase_firetv_id_mismatch='True'
                                                                         purchase_firetv_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("firetv_id")])

                                                                 if hh.get("appletv_id") is not None:
                                                                     if purchase_source_program_id in hh.get("appletv_id").encode('utf-8'):
                                                                         purchase_appletv_id_match="True"
                                                                         purchase_appletv_id_match_=['True']
                                                                     else:
                                                                         purchase_appletv_id_mismatch='True'
                                                                         purchase_appletv_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("appletv_id")])

                                                                 if hh.get("androidtv_id") is not None:
                                                                     if purchase_source_program_id in hh.get("androidtv_id").encode('utf-8'):
                                                                         purchase_androidtv_id_match="True"
                                                                         purchase_androidtv_id_match_=['True']
                                                                     else:
                                                                         purchase_androidtv_id_mismatch='True'
                                                                         purchase_androidtv_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("androidtv_id")])

                                                                 if mm.get("formats"):
                                                                     for kk in range (g,len(mm.get("formats"))):
                                                                         if mm.get("formats")[kk].get("format")==hh.get("quality"):
                                                                             format_match='True'
                                                                             format_match_=['True']
                                                                         else:
                                                                             g=g+1
                                                                             for ll in range(g,len(mm.get("formats"))):
                                                                                 if mm.get("formats")[ll].get("format")==hh.get("quality"):
                                                                                     format_match='True'
                                                                                     format_match_=['True']
                                                                                     g=g+1

                                                                                 else:
                                                                                     g=g+1
                                                                                     if g<len(mm.get("formats")):
                                                                                         for ll in range(g,len(mm.get("formats"))):
                                                                                             if mm.get("formats")[ll].get("format")==hh.get("quality"):
                                                                                                 format_match='True'
                                                                                                 format_match_=['True']
                                                                                                 g=g+1
                                                                                             else:
                                                                                                 g=g+1
                                                                                                 if g<len(mm.get("formats")):
                                                                                                     for ll in range(g,len(mm.get("formats"))):
                                                                                                         if mm.get("formats")[ll].get("format")==hh.get("quality"):
                                                                                                             format_match='True'
                                                                                                             format_match_=['True']
                                                                                                             g=g+1

                                                                                                         else:
                                                                                                             format_mismatch='True'
                                                                                                             format_mismatch_.append([mm.get("link"),"worng format:",hh.get("quality")])
                                                                                                 else:
                                                                                                     format_mismatch='True'
                                                                                                     format_mismatch_.append([mm.get("link"),"worng format:",hh.get("quality")])
                                                                                     else:
                                                                                         format_mismatch='True'
                                                                                         format_mismatch_.append([mm.get("link"),"worng format:",hh.get("quality")])
                                                                    # for kk in range (g,len(mm.get("formats"))):
                                                                         if hh.get("price")=='0.0':
                                                                             price='0.00'
                                                                         if price=='0.00':
                                                                             if mm.get("formats")[kk].get("price")==price:
                                                                                 price_match='True'
                                                                                 price_match_=['True']

                                                                             else:
                                                                                 price_mismatch='True'
                                                                                 price_mismatch_.append([mm.get("link"),"worng price:",hh.get("price")])

                                                                         else:
                                                                             if mm.get("formats")[kk].get("price")==hh.get("price"):
                                                                                 price_match='True'
                                                                                 price_match_=['True']

                                                                             else:
                                                                                 price_mismatch='True'
                                                                                 price_mismatch_.append([mm.get("link"),"worng price:",hh.get("price")])
                                                                         if mm.get("formats")[kk].get("type")=='purchase':
                                                                             purchase_type='buy'
                                                                         else:
                                                                             purchase_type='rent'
                                                                         if purchase_type==hh.get("purchase_type"):
                                                                             purchase_type_match='True'
                                                                             purchase_type_match_=['True']
                                                                             g=g+1
                                                                             break
                                                                         else: 
                                                                             purchase_type_mismatch='True'
                                                                             purchase_type_mismatch_.append([mm.get("link"),"worng purchase_type:",hh.get("purchase_type")])
                                                                             g=g+1
                                                                             break


                                                 else:
                                                     print("No web sources")
                                                     link_mismatch='True'
                                                     link_mismatch_.append(jj)




                                     if data_resp_gb.get("subscription_web_sources")!=[]:
                                         a=data_resp_gb.get("subscription_web_sources")
                                      #import pdb;pdb.set_trace()
                                         if a:
                                             for x in a:
                                                 b=(x.get("link")).encode("utf-8")
                                                 c=(x.get("source")).encode("utf-8")
                                                 if 'amazon_prime' in c or 'amazon_buy' in c:
                                                     c='amazon'
                                                 if 'netflix' in c:
                                                     c='netflixusa'
                                                 if c=='hbo':
                                                     c='hbogo'
                                                 if c=='hbo_now':
                                                     c='hbonow'
                                                 if c=='google_play':
                                                     c='googleplay'
                                                 if c=='hulu_plus':
                                                     c='hulu'
                                                 if c =='verizon_on_demand':
                                                     c='verizon'
                                                 if c=='showtime_subscription':
                                                     c='showtime'
                                                 if 'vuduapp' in b:
                                                     subscription_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                                 if 'hbo.hbonow' in b or 'hbonow://asset' in b:
                                                     subscription_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                                 if 'play.google' in b:
                                                     try:
                                                         subscription_source_program_id=re.findall("\w+-\w+.*?",b)[0]
                                                     except IndexError:
                                                         subscription_source_program_id=re.findall("\w+.*?", b)[-1:][0]

                                                 if 'aiv://aiv/play' in b:
                                                     subscription_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                                 if '//itunes.apple.com/us/movie' in b:
                                                     subscription_source_program_id=re.findall("\d+", b)[0]
                                                 if '//www.amazon.com/gp' in b:
                                                     subscription_source_program_id=re.findall("\w+\d+\w+", b)[0]
                                                 if '//click.linksynergy.com/' in b:
                                                     subscription_source_program_id=re.findall("\d+", b)[-1:][0]
                                                 if '//www.youtube.com/w' in b:
                                                     subscription_source_program_id=re.findall("\w+\d+\w+", b)[0]
                                                 if '//www.verizon.com/Ondemand/' in b:
                                                     subscription_source_program_id=re.findall("\w+\d+\w+", b)[0]
                                                 if '//play.hbonow.com/feature/' in b:
                                                     try:
                                                         a10=re.findall("\w+.*?", b)
                                                         subscription_source_program_id=':'.join(map(str, [a10[i] for i in range(5,9)]))
                                                     except IndexError:
                                                         subscription_source_program_id=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                                 if 'netflix.com' in b:
                                                     subscription_source_program_id=re.findall("\w+.*?",b)[-1:][0]
                                                 if 'http://www.showtime.com/#' in b:
                                                     subscription_source_program_id=re.findall("\w+.*?",b)[-1:][0]
                                                 if 'http://www.hulu.com/watch' in b:
                                                     try:
                                                         a14=re.findall("\w+.*?",b)[-5:]
                                                         subscription_source_program_id='-'.join(map(str,[a14[i] for i in range(0,len(a14))]))
                                                     except IndexError:
                                                         subscription_source_program_id=re.findall("\w+.*?",b)[-1:][0]
                                                 if '/play.hbonow.com/episode/' in b:
                                                     try:
                                                         subscription_source_program_id=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                                     except IndexError:
                                                         a15=re.findall("\w+.*?.-*?", b)
                                                         subscription_source_program_id=''.join(map(str, [a15[i] for i in range(5,len(a15))]))
 
                                                 for hh in projectx_videos:
                                                     if hh.get("fetched_from")=='GuideBox' and hh.get("link").get("uri").encode()==b:
                                                         try: 
                                                             if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("constraints") is not None:
                                                                 subscription_link_match='True'
                                                             elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("constraints") is not None:
                                                                 subscription_link_match='True'
                                                             else:
                                                                 subscription_link_mismatch='True'
                                                                 subscription_link_mismatch_.append(hh.get("link").get("uri").encode())
 
                                                             if hh.get("source_program_id") is not None:
      
                                                                 if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("source_program_id").encode()==subscription_source_program_id and hh.get("constraints") is not None:
                                                                     subscription_source_program_id_match="True"
                                                                 elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("source_program_id").encode()==subscription_source_program_id and hh.get("constraints") is not None:
                                                                     subscription_source_program_id_match="True"
                                                                 else:
                                                                     subscription_source_program_id_mismatch='True'
                                                                     subscription_source_program_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_source_program_id:",hh.get("source_program_id")]) 

                                                             if hh.get("launch_id") is not None:

                                                                 if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("launch_id").encode()==subscription_source_program_id and hh.get("constraints") is not None:
                                                                     subscription_launch_id_match="True"
                                                                 elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("launch_id").encode()==subscription_source_program_id and hh.get("constraints") is not None:
                                                                     subscription_launch_id_match="True"
                                                                 else:
                                                                     subscription_launch_id_mismatch='True'
                                                                     subscription_launch_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_launch_id:",hh.get("launch_id")])

                                                             if hh.get("roku_id") is not None:
    
                                                                 if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("roku_id") and hh.get("constraints") is not None:
                                                                     subscription_roku_id_match="True"
                                                                 elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("roku_id") and hh.get("constraints") is not None:
                                                                     subscription_roku_id_match="True"
                                                                 else:
                                                                     subscription_roku_id_mismatch='True'
                                                                     subscription_roku_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_roku_id:",hh.get("roku_id")])

                                                             if hh.get("firetv_id") is not None:
 
                                                                 if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("firetv_id") and hh.get("constraints") is not None:
                                                                     subscription_firetv_id_match="True"
                                                                 elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("firetv_id") and hh.get("constraints") is not None:
                                                                     subscription_firetv_id_match="True"
                                                                 else:
                                                                     subscription_firetv_id_mismatch='True'
                                                                     subscription_firetv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_firetv_id:",hh.get("firetv_id")])
 
                                                             if hh.get("appletv_id") is not None:
 
                                                                 if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("appletv_id") and hh.get("constraints") is not None:
                                                                     subscription_appletv_id_match="True"
                                                                 elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("appletv_id") and hh.get("constraints") is not None:
                                                                     subscription_appletv_id_match="True"
                                                                 else:
                                                                     subscription_appletv_id_mismatch='True'
                                                                     subscription_appletv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_appletv_id:",hh.get("appletv_id")])

                                                             if hh.get("androidtv_id") is not None:

                                                                 if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("androidtv_id") and hh.get("constraints") is not None:
                                                                     subscription_androidtv_id_match="True"
                                                                 elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("amdroidtv_id") and hh.get("constraints") is not None:
                                                                     subscription_androidtv_id_match="True"
                                                                 else:
                                                                     subscription_androidtv_id_mismatch='True'
                                                                     subscription_androidtv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_androidtv_id:",hh.get("androidtv_id")])
                                                         except TypeError:
                                                             pass




                                     if data_resp_gb.get("tv_everywhere_web_sources")!=[]:
                                         a=data_resp_gb.get("tv_everywhere_web_sources")
                                    #     import pdb;pdb.set_trace()
                                         if a:
                                             for x in a:
                                                 b=(x.get("link")).encode("utf-8")
                                                 c=(x.get("source")).encode("utf-8")
                                                 if 'amazon_prime' in c or 'amazon_buy' in c:
                                                     c='amazon'
                                                 if 'netflix' in c:
                                                     c='netflixusa'
                                                 if c=='hbo':
                                                     c='hbogo'
                                                 if c=='hbo_now':
                                                     c='hbonow'
                                                 if c=='starz_tveverywhere':
                                                     c='starz'
                                                 if "hbogo://deeplink/" in b or 'hbonow://asset' in b: #"""****************"""
                                                     tv_everywhere_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                                 if "starz://play" in b or "//www.starz.com/" in b:
                                                     tv_everywhere_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                                 if "//play.hbogo.com/feature" in b:
                                                     try:
                                                         tv_everywhere_source_program_id=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                                     except IndexError:
                                                         try:
                                                             a3=re.findall("\w+.*?", b)
                                                             tv_everywhere_source_program_id=':'.join(map(str, [a3[i] for i in range(5,9)]))
                                                         except IndexError:
                                                             tv_everywhere_source_program_id=re.findall("\w+.*?",b)[-1:][0]
                                                 if 'http://www.showtimeanytime.com/#' in b:
                                                     tv_everywhere_source_program_id=re.findall("\d+\w+", b)[0]
                                                 if '/play.hbogo.com/episode/' in b:
                                                     try:
                                                         a6=re.findall("\w+.*?.-*?", b)
                                                         tv_everywhere_source_program_id=''.join(map(str, [a6[i] for i in range(5,len(a6))]))
                                                     except IndexError:
                                                         try:
                                                             tv_everywhere_source_program_id=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                                         except IndexError:
                                                             tv_everywhere_source_program_id=re.findall("\w+.*?",b)[-1:][0]                  

                                                 for hh in projectx_videos:
                                                     try:
                                                         if hh.get("fetched_from")=='GuideBox' and "cable_subscription" in hh.get("constraints"):
                                                             if hh.get("link").get("uri").encode()==b:
                                                                 tv_everywhere_link_match='True'
                                                             else:
                                                                 tv_everywhere_link_mismatch='True'
                                                                 tv_everywhere_link_mismatch_.append(hh.get("link").get("uri").encode())
                                                             if hh.get("link").get("uri").encode()==b and hh.get("source_program_id").encode()==tv_everywhere_source_program_id:
                                                                 tv_everywhere_source_program_id_match ='True'
                                                             else:
                                                                 tv_everywhere_source_program_id_mismatch='True'
                                                                 tv_everywhere_source_program_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_source_program_id:",hh.get("source_program_id")])
                                                             if hh.get("link").get("uri").encode()==b and hh.get("launch_id").encode()==tv_everywhere_source_program_id:
                                                                 tv_everywhere_launch_id_match ='True'
                                                             else:
                                                                 tv_everywhere_launch_id_mismatch='True'
                                                                 tv_everywhere_launch_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_launch_id:",hh.get("launch_id")])
                                                             if hh.get("link").get("uri").encode()==b and tv_everywhere_source_program_id in hh.get("roku_id"):
                                                                 tv_everywhere_roku_id_match ='True'
                                                             else:
                                                                 tv_everywhere_roku_id_mismatch='True'
                                                                 tv_everywhere_roku_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_roku_id:",hh.get("roku_id")])
                                                             if hh.get("link").get("uri").encode()==b and tv_everywhere_source_program_id in hh.get("firetv_id"):
                                                                 tv_everywhere_firetv_id_match ='True'
                                                             else:
                                                                 tv_everywhere_firetv_id_mismatch='True'
                                                                 tv_everywhere_firetv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_firetv_id:",hh.get("firetv_id")])
                                                             if hh.get("link").get("uri").encode()==b and tv_everywhere_source_program_id in hh.get("appletv_id"):
                                                                 tv_everywhere_appletv_id_match ='True'
                                                             else:
                                                                 tv_everywhere_appletv_id_mismatch='True'
                                                                 tv_everywhere_appletv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_appletv_id:",hh.get("appletv_id")])
                                                             if hh.get("link").get("uri").encode()==b and tv_everywhere_source_program_id in hh.get("androidtv_id"):
                                                                 tv_everywhere_androidtv_id_match ='True'
                                                             else:
                                                                 tv_everywhere_androidtv_id_mismatch='True'
                                                                 tv_everywhere_androidtv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_androidtv_id:",hh.get("androidtv_id")]) 
                                                     except TypeError:
                                                        pass     

                                     if link_mismatch=='True' or purchase_source_program_id_mismatch=='True' or format_mismatch=='True' or price_mismatch=='True' or purchase_type_mismatch=='True' or subscription_link_mismatch=='True' or tv_everywhere_link_mismatch=='True' or tv_everywhere_source_program_id_mismatch=='True' or subscription_source_program_id_mismatch=='True' or tv_everywhere_androidtv_id_mismatch=='True' or tv_everywhere_appletv_id_mismatch=='True' or tv_everywhere_firetv_id_mismatch=='True' or tv_everywhere_roku_id_mismatch=='True' or tv_everywhere_launch_id_mismatch=='True' or subscription_androidtv_id_mismatch=='True' or subscription_appletv_id_mismatch=='True' or subscription_firetv_id_mismatch=='True' or subscription_roku_id_mismatch=='True' or subscription_launch_id_mismatch=='True' or purchase_androidtv_id_mismatch=='True' or purchase_appletv_id_mismatch=='True' or purchase_firetv_id_mismatch=='True' or purchase_roku_id_mismatch=='True' or purchase_launch_id_mismatch=='True':
                                         v=v+1
                                         writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"show_type":show_type,"projectx_id_gb":str(projectx_id_gb),"link_mismatch":link_mismatch,"Wrong link":link_mismatch_,"purchase_source_program_id_mismatch":purchase_source_program_id_mismatch,"Wrong_source_program_id":purchase_source_program_id_mismatch_,"format_mismatch":format_mismatch,"Wrong format":format_mismatch_,"price_mismatch":price_mismatch,"Wrong price":price_mismatch_,"purchase_type_mismatch":purchase_type_mismatch,"wrong purchase type":purchase_type_mismatch_,"purchase_launch_id_mismatch":purchase_launch_id_mismatch,"wrong purchase_launch_id":purchase_launch_id_mismatch_,"purchase_roku_id_mismatch":purchase_roku_id_mismatch,"wrong purchase_roku_id":purchase_roku_id_mismatch_,"purchase_firetv_id_mismatch":purchase_firetv_id_mismatch,"wrong purchase_firetv_id":purchase_firetv_id_mismatch_,"purchase_appletv_id_mismatch":purchase_appletv_id_mismatch,"wrong purchase_appletv_id":purchase_appletv_id_mismatch_,"purchase_androidtv_id_mismatch":purchase_androidtv_id_mismatch,"wrong purchase_androidtv_id":purchase_androidtv_id_mismatch_,"subscription_link_mismatch":subscription_link_mismatch,"Wrong subscription_link":subscription_link_mismatch_,"tv_everywhere_link_mismatch":tv_everywhere_link_mismatch,"Wrong tv_everywhere_link":tv_everywhere_link_mismatch_,"tv_everywhere_source_program_id_mismatch":tv_everywhere_source_program_id_mismatch,"Wrong tv_everywhere_source_program_id":tv_everywhere_source_program_id_mismatch_,"tv_everywhere_launch_id_mismatch":tv_everywhere_launch_id_mismatch,"Wrong tv_everywhere_launch_id":tv_everywhere_launch_id_mismatch_,"tv_everywhere_roku_id_mismatch":tv_everywhere_roku_id_mismatch,"Wrong tv_everywhere_roku_id":tv_everywhere_roku_id_mismatch_,"tv_everywhere_firetv_id_mismatch":tv_everywhere_firetv_id_mismatch,"Wrong tv_everywhere_firetv_id":tv_everywhere_firetv_id_mismatch_,"tv_everywhere_appletv_id_mismatch":tv_everywhere_appletv_id_mismatch,"Wrong tv_everywhere_appletv_id":tv_everywhere_appletv_id_mismatch_,"tv_everywhere_androidtv_id_mismatch":tv_everywhere_androidtv_id_mismatch,"Wrong tv_everywhere_androidtv_id":tv_everywhere_androidtv_id_mismatch_,"subscription_source_program_id_mismatch":subscription_source_program_id_mismatch,"Wrong subscription_source_program_id":subscription_source_program_id_mismatch_,"subscription_launch_id_mismatch":subscription_launch_id_mismatch,"wrong subscription_launch_id":subscription_launch_id_mismatch_,"subscription_roku_id_mismatch":subscription_roku_id_mismatch,"wrong subscription_roku_id":subscription_roku_id_mismatch_,"subscription_firetv_id_mismatch":subscription_firetv_id_mismatch,"wrong subscription_firetv_id":subscription_firetv_id_mismatch_,"subscription_appletv_id_mismatch":subscription_appletv_id_mismatch,"wrong subscription_appletv_id":subscription_appletv_id_mismatch_,"subscription_androidtv_id_mismatch":subscription_androidtv_id_mismatch,"wrong subscription_androidtv_id":subscription_androidtv_id_mismatch_,"Result for Ott_link population":'Fail'})
                                     else:
                                         t=t+1
                                         writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"show_type":show_type,"projectx_id_gb":str(projectx_id_gb),"link_mismatch":link_mismatch,"Wrong link":link_mismatch_,"purchase_source_program_id_mismatch":purchase_source_program_id_mismatch,"Wrong_source_program_id":purchase_source_program_id_mismatch_,"format_mismatch":format_mismatch,"Wrong format":format_mismatch_,"price_mismatch":price_mismatch,"Wrong price":price_mismatch_,"purchase_type_mismatch":purchase_type_mismatch,"wrong purchase type":purchase_type_mismatch_,"purchase_launch_id_mismatch":purchase_launch_id_mismatch,"wrong purchase_launch_id":purchase_launch_id_mismatch_,"purchase_roku_id_mismatch":purchase_roku_id_mismatch,"wrong purchase_roku_id":purchase_roku_id_mismatch_,"purchase_firetv_id_mismatch":purchase_firetv_id_mismatch,"wrong purchase_firetv_id":purchase_firetv_id_mismatch_,"purchase_appletv_id_mismatch":purchase_appletv_id_mismatch,"wrong purchase_appletv_id":purchase_appletv_id_mismatch_,"purchase_androidtv_id_mismatch":purchase_androidtv_id_mismatch,"wrong purchase_androidtv_id":purchase_androidtv_id_mismatch_,"subscription_link_mismatch":subscription_link_mismatch,"Wrong subscription_link":subscription_link_mismatch_,"tv_everywhere_link_mismatch":tv_everywhere_link_mismatch,"Wrong tv_everywhere_link":tv_everywhere_link_mismatch_,"tv_everywhere_source_program_id_mismatch":tv_everywhere_source_program_id_mismatch,"Wrong tv_everywhere_source_program_id":tv_everywhere_source_program_id_mismatch_,"tv_everywhere_launch_id_mismatch":tv_everywhere_launch_id_mismatch,"Wrong tv_everywhere_launch_id":tv_everywhere_launch_id_mismatch_,"tv_everywhere_roku_id_mismatch":tv_everywhere_roku_id_mismatch,"Wrong tv_everywhere_roku_id":tv_everywhere_roku_id_mismatch_,"tv_everywhere_firetv_id_mismatch":tv_everywhere_firetv_id_mismatch,"Wrong tv_everywhere_firetv_id":tv_everywhere_firetv_id_mismatch_,"tv_everywhere_appletv_id_mismatch":tv_everywhere_appletv_id_mismatch,"Wrong tv_everywhere_appletv_id":tv_everywhere_appletv_id_mismatch_,"tv_everywhere_androidtv_id_mismatch":tv_everywhere_androidtv_id_mismatch,"Wrong tv_everywhere_androidtv_id":tv_everywhere_androidtv_id_mismatch_,"subscription_source_program_id_mismatch":subscription_source_program_id_mismatch,"Wrong subscription_source_program_id":subscription_source_program_id_mismatch_,"subscription_launch_id_mismatch":subscription_launch_id_mismatch,"wrong subscription_launch_id":subscription_launch_id_mismatch_,"subscription_roku_id_mismatch":subscription_roku_id_mismatch,"wrong subscription_roku_id":subscription_roku_id_mismatch_,"subscription_firetv_id_mismatch":subscription_firetv_id_mismatch,"wrong subscription_firetv_id":subscription_firetv_id_mismatch_,"subscription_appletv_id_mismatch":subscription_appletv_id_mismatch,"wrong subscription_appletv_id":subscription_appletv_id_mismatch_,"subscription_androidtv_id_mismatch":subscription_androidtv_id_mismatch,"wrong subscription_androidtv_id":subscription_androidtv_id_mismatch_,"Result for Ott_link population":'Pass'})
                                     print("\n")
                                     print(result_sheet,"thread_name:", name, "total count of GB ID: ", total ,"Not ingested Gb_id :", w, "Ott_link pass: ",t,"missing ott_contentsand additional/duplicates ott_contents :", v, "missing ott_contents: ", v, "Total fail : ",v, "Total pass: ", t)
                                     print datetime.datetime.now()

                    else:
                        total=total+1
                        for x in data_resp:
                            projectx_id_gb=str(x["projectx_id"])	           
                        Px_ott_url_purchase=[]
                        price=''
                        purchase_source_program_id=''
                        Gb_ott_url_purchase=[]
                        purchase_source_program_id_match=''
                        purchase_source_program_id_match_=[]
                        purchase_source_program_id_mismatch =''
                        purchase_source_program_id_mismatch_ =[] 
                        purchase_launch_id_match=''
                        purchase_launch_id_match_=[]
                        purchase_launch_id_mismatch=''
                        purchase_launch_id_mismatch_=[]
                        purchase_roku_id_match=''
                        purchase_roku_id_match_=[]  
                        purchase_roku_id_mismatch=''
                        purchase_roku_id_mismatch_=[]
                        purchase_firetv_id_match=''
                        purchase_firetv_id_match_=[]
                        purchase_firetv_id_mismatch_=[]
                        purchase_firetv_id_mismatch=''
                        purchase_appletv_id_match=''
                        purchase_appletv_id_match_=[]
                        purchase_appletv_id_mismatch=''
                        purchase_appletv_id_mismatch_=[]
                        purchase_androidtv_id_match=''
                        purchase_androidtv_id_match_=[]
                        purchase_androidtv_id_mismatch =''
                        purchase_androidtv_id_mismatch_=[]

                        projectx_videos=''
                        format_match=''
                        format_match_=[]
                        format_mismatch=''
                        format_mismatch_=[]
                        price_match=''
                        price_match_=[]
                        price_mismatch=''
                        price_mismatch_ =[]
                        purchase_type_match=''
                        purchase_type_match_=[]
                        purchase_type_mismatch=''
                        purchase_type_mismatch_=[]
                        projectx_videos_link_flag=''
                        link_match=''
                        link_mismatch=''
                        link_mismatch_=[]
                        subscription_source_program_id=''
                        subscription_link_mismatch_=[]
                        subscription_link_match=''
                        subscription_link_mismatch=''
                        subscription_source_program_id_match=''
                        subscription_source_program_id_mismatch='' 
                        subscription_source_program_id_mismatch_=[]
                        subscription_launch_id_match=''
                        subscription_launch_id_mismatch =''
                        subscription_launch_id_mismatch_=[]
                        subscription_roku_id_match=''
                        subscription_roku_id_mismatch =''
                        subscription_roku_id_mismatch_=[]
                        subscription_firetv_id_match=''
                        subscription_firetv_id_mismatch=''
                        subscription_firetv_id_mismatch_=[]
                        subscription_appletv_id_match=''
                        subscription_appletv_id_mismatch=''
                        subscription_appletv_id_mismatch_=[]
                        subscription_androidtv_id_match=''
                        subscription_androidtv_id_mismatch=''
                        subscription_androidtv_id_mismatch_=[]  
                        tv_everywhere_source_program_id=''
                        tv_everywhere_launch_id_match=''
                        tv_everywhere_launch_id_mismatch  =''
                        tv_everywhere_launch_id_mismatch_=[]
                        tv_everywhere_roku_id_match=''
                        tv_everywhere_roku_id_mismatch=''
                        tv_everywhere_roku_id_mismatch_=[]
                        tv_everywhere_firetv_id_match=''
                        tv_everywhere_firetv_id_mismatch=''
                        tv_everywhere_firetv_id_mismatch_=[]
                        tv_everywhere_appletv_id_match =''
                        tv_everywhere_appletv_id_mismatch=''
                        tv_everywhere_appletv_id_mismatch_=[]
                        tv_everywhere_androidtv_id_match=''
                        tv_everywhere_androidtv_id_mismatch=''
                        tv_everywhere_androidtv_id_mismatch_=[]
                        tv_everywhere_link_match=''
                        tv_everywhere_link_mismatch_=[]
                        tv_everywhere_link_mismatch=''
                        tv_everywhere_source_program_id_mismatch=''
                        tv_everywhere_source_program_id_mismatch_=[]
                        tv_everywhere_source_program_id_match='' 
                        print("show_type : ", show_type)
                        print("gb_id : ", gb_id)
                        print("gb_sm_id: ", gb_sm_id)
 
                        j=0  
                        if show_type=='SE':
                            Guidebox_dump_api="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=SE"%gb_id
                            response_GB_api=urllib2.Request(Guidebox_dump_api)
                            response_GB_api.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp=urllib2.urlopen(response_GB_api)
                            data_GB=resp.read()
                            data_resp_gb=json.loads(data_GB)
  
                            projectx_url="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%eval(projectx_id_gb)
                            response1=urllib2.Request(projectx_url)
                            response1.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp1=urllib2.urlopen(response1)
                            data1=resp1.read()
                            data_resp1=json.loads(data1) 
                        else:
                            Guidebox_dump_api="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=MO"%gb_id
                            response_GB_api=urllib2.Request(Guidebox_dump_api)
                            response_GB_api.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp=urllib2.urlopen(response_GB_api)
                            data_GB=resp.read()
                            data_resp_gb=json.loads(data_GB)

                            projectx_url="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%eval(projectx_id_gb)
                            response1=urllib2.Request(projectx_url)
                            response1.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp1=urllib2.urlopen(response1)
                            data1=resp1.read()
                            data_resp1=json.loads(data1) 
                        for bb in data_resp1:
                            projectx_videos=bb.get("videos")
                        dict7_=dict()
                        k=0
                        g=0 
                        if data_resp_gb.get("purchase_web_sources")!=[]:
                            import pdb;pdb.set_trace() 
                            a=data_resp_gb.get("purchase_web_sources") 
                            if a!=[]:
                                for x in data_resp1:
                                    if x.get("videos"):
                                        projectx_videos_link_flag=='True'
                                        for mm in x.get("videos"):
                                            if mm.get("fetched_from")=='GuideBox' and mm.get("constraints") is None and mm.get("constraints")!=[]:
                                                Px_ott_url_purchase.append(mm.get("link").get("uri").encode("utf-8"))
                                    else:
                                        projectx_videos_link_flag=='True'

                                for nn in a :
                                    Gb_ott_url_purchase.append(nn.get("link"))
                                
                                if Px_ott_url_purchase:   
                                    for jj in Px_ott_url_purchase:       
                                        if jj in Gb_ott_url_purchase:
                                            link_match='True'
                                            for mm in a:
                                                for hh in projectx_videos:
                                                    if hh.get("fetched_from")=='GuideBox' and hh.get("link").get("uri").encode()==mm.get("link") and hh.get("constraints") is None and hh.get("constraints")!=[]:                                                                             
                                                        if 'vuduapp' in mm.get("link"):
                                                            purchase_source_program_id=re.findall("\w+.*?", mm.get("link"))[-1:][0]
                                                        if 'hbo.hbonow' in mm.get("link") or 'hbonow://asset' in mm.get("link"):
                                                            purchase_source_program_id=re.findall("\w+.*?", mm.get("link"))[-1:][0]
                                                        try:
                                                            if 'play.google' in mm.get("link"):
                              	                                purchase_source_program_id=re.findall("\w+-.*?\-\-\w+.*?\w+\-(\w+.*)", mm.get("link"))[0]
                                                        except IndexError:
                                                            try:
                                                                purchase_source_program_id=re.findall("\w+.*?\-(\-\w+.*?\w+.*?\w.*?\w)", mm.get("link"))[0]
                                                            except IndexError:
                                                                try:
                         	                                    purchase_source_program_id=re.findall("\w+.*?\-(\w+.*?\w+)", mm.get("link"))[-1:][0]
                                                                except IndexError:
                                                                    try:
                                                                        purcahse_source_program_id=re.findall("\w+-.*?\-(\-\w+.*?)", mm.get("link"))[-1:][0]
                                                                    except IndexError:
                             	                                        try:
                    	        	                                    purchase_source_program_id=re.findall("\w+.*?\-(\w+.*?\w+\w+.\w.-*?\d*\w?)", mm.get("link"))[-1][0]
                    			                                except IndexError:
                                                                            try:
            	  		                                                purchase_source_program_id=re.findall("\w+.*?\-(\w+.*?\w+.-*\w.*?\w+)", mm.get("link"))[-1:][0]  #### 
                                                                            except IndexError:
                                                                                purchase_source_program_id=re.findall("w+.*?",mm.get("link"))[-1:][0]
                                                                          
                                    
                                                        try:
                                                            if '//itunes.apple.com/us/tv-season' in mm.get("link"):
              			                                purchase_source_program_id=re.findall("\d+", mm.get("link"))[-2:-1][0]
                                                        except IndexError:
        	                                            try:
            	                                                purchase_source_program_id=re.findall("\d+", mm.get("link"))[1:-2][0]
                                                            except IndexError:
                                                                purchase_source_program_id=re.findall("\d+", mm.get("link"))[0]
                                                        try:
                                                            if '//itunes.apple.com/us/movie' in mm.get("link"):
                                                                purchase_source_program_id=re.findall("\d+",mm.get("link"))[0:-2][2:][1]
                                                        except IndexError:
                                                            try:
                                                                purchase_source_program_id=re.findall("\d+", mm.get("link"))[1:-2][1]
                                                            except IndexError:
                                                                try:
                                                                    purchase_source_program_id=re.findall("\d+", mm.get("link"))[0:-2][1:2][0]
                                                                except IndexError:
                                                                    purchase_source_program_id=re.findall("\d+", mm.get("link"))[0]
                                                        if '//www.amazon.com/gp' in mm.get("link"):
                                                            purchase_source_program_id=re.findall("\w+\d+\w+", mm.get("link"))[0]
                                                        if '//click.linksynergy.com/' in mm.get("link"):
                                                            purchase_source_program_id=re.findall("\d+.*?", mm.get("link"))[-1:][0]
                                                        try: 
                                                            if '//www.youtube.com/w' in mm.get("link"):
                                                                purchase_source_program_id=re.findall("\w+-.*?\w+-\w+",mm.get("link"))[0]
                             	                        except IndexError:
			                                    try:
                                                                purchase_source_program_id=re.findall("\w+-\w+.*?",mm.get("link"))[0] 
                                                            except IndexError:
                                                                try:
                                                                    purchase_source_program_id=re.findall("\-\w+.*?",mm.get("link"))[0]
                             	                                except IndexError:
		                	                            try:
	   		                                                purchase_source_program_id=re.findall("\w+.*?\w+", mm.get("link"))[-1:][0]
                                                                    except IndexError:
                                                                        try:
                                                                            purchase_source_program_id=re.findall("\w+\d+\w+", mm.get("link"))[0]
                                                                        except IndexError:
                                                                            purchase_source_program_id=re.findall("\w+.*?",mm.get("link"))[-1:][0]
                                                        if '//www.verizon.com/' in mm.get("link"):
            			                            try:
                                                                purchase_source_program_id=re.findall("\w+\d+", mm.get("link"))[-1:][0]   #"""***************"""
                                                            except IndexError:
                         	                                purchase_source_program_id=re.findall("\d+", mm.get("link"))[-1]
                         	                        if 'https://www.paramountmovies.com' in mm.get("link"):
                                                            try:
                                                                purcahse_source_program_id=re.findall("\w+\d+" ,mm.get("link"))[-1:][0]
                                                            except IndexError:
                                                                try:
                                                                    purchase_source_program_id=re.findall("\w+\d+\w+" ,mm.get("link"))[-1:][0]
                                                                except IndexError:
            		  	                                    try:
                                                                        purchase_source_program_id=re.findall("\w+" ,mm.get("link"))[-1:][0]
                                                                    except IndexError:
            			                                        pass

                                                        if '//www.verizon.com/Ondemand/Movies/' in mm.get("link"):
                                                            purchase_source_program_id=re.findall("\w+\d+\w+", b)[0]
                                                        if 'https://store.sonyentertainmentnetwork.com/' in mm.get("link"):
                                                            purchase_source_program_id=re.findall("\w+-\w+-\w+.*?",mm.get("link"))[0]
        		                                if 'https://www.mgo.com/' in mm.get("link"):
                                                            purchase_source_program_id=re.findall("\w+\d+\w+",mm.get("link"))[0]
 #                                                       import pdb;pdb.set_trace()
                                                        if hh.get("source_program_id").encode('utf-8')==purchase_source_program_id:
                                                            purchase_source_program_id_match='True'
                                                            purchase_source_program_id_match_=['True']
                                                        else:
                                                            purchase_source_program_id_mismatch='True'
                                                            purchase_source_program_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("source_program_id")])
                                                        if hh.get("launch_id") is not None:
                                                            if hh.get("launch_id").encode('utf-8')==purchase_source_program_id:
                                                                purchase_launch_id_match="True" 
                                                                purchase_launch_id_match_=['True']
                                                            else:
                                                                purchase_launch_id_mismatch='True'
                                                                purchase_launch_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("launch_id")])
                                                        if hh.get("roku_id") is not None:
                                                            if purchase_source_program_id in hh.get("roku_id").encode('utf-8'):
                                                                purchase_roku_id_match="True"
                                                                purchase_roku_id_match_=['True']
                                                            else:
                                                                purchase_roku_id_mismatch='True'
                                                                purchase_roku_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("roku_id")]) 
                                                        if hh.get("firetv_id") is not None:

                                                            if purchase_source_program_id in hh.get("firetv_id").encode('utf-8'):
                                                                purchase_firetv_id_match="True"
                                                                purchase_firetv_id_match_=['True']
                                                            else:
                                                                purchase_firetv_id_mismatch='True'
                                                                purchase_firetv_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("firetv_id")])
                                                        if hh.get("appletv_id") is not None: 
                                                            if purchase_source_program_id in hh.get("appletv_id").encode('utf-8'):
                                                                purchase_appletv_id_match="True"
                                                                purchase_appletv_id_match_=['True']
                                                            else:
                                                                purchase_appletv_id_mismatch='True'
                                                                purchase_appletv_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("appletv_id")])
                                                        if hh.get("androidtv_id") is not None:
                                                            if purchase_source_program_id in hh.get("androidtv_id").encode('utf-8'):
                                                                purchase_androidtv_id_match="True"
                                                                purchase_androidtv_id_match_=['True']
                                                            else:
                                                                purchase_androidtv_id_mismatch='True'
                                                                purchase_androidtv_id_mismatch_.append([mm.get("link"),"wrong purchase_source_program_id: ", hh.get("androidtv_id")])

                                                        if mm.get("formats"):
                                                            for kk in range (g,len(mm.get("formats"))):
                                                                if mm.get("formats")[kk].get("format")==hh.get("quality"):
                                                                    format_match='True'
                                                                    format_match_=['True']
                                                                else:
                                                                    g=g+1
                                                                    for ll in range(g,len(mm.get("formats"))):
                                                                        if mm.get("formats")[ll].get("format")==hh.get("quality"):
                                                                            format_match='True'
                                                                            format_match_=['True']
                                                                            g=g+1
                                                                            
                                                                        else: 
                                                                            g=g+1
                                                                            if g<len(mm.get("formats")):
                                                                                for ll in range(g,len(mm.get("formats"))):
                                                                                    if mm.get("formats")[ll].get("format")==hh.get("quality"):
                                                                                        format_match='True'
                                                                                        format_match_=['True']
                                                                                        g=g+1
                                                                                        
                                                                                    else:
                                                                                        g=g+1
                                                                                        if g<len(mm.get("formats")):
                                                                                            for ll in range(g,len(mm.get("formats"))):
                                                                                                if mm.get("formats")[ll].get("format")==hh.get("quality"):
                                                                                                    format_match='True'
                                                                                                    format_match_=['True']
                                                                                                    g=g+1
                                                                                                       
                                                                                                else:
                                                                                                    format_mismatch='True'
                                                                                                    format_mismatch_.append([mm.get("link"),"worng format:",hh.get("quality")])       
                                                                                        else:
                                                                                            format_mismatch='True'
                                                                                            format_mismatch_.append([mm.get("link"),"worng format:",hh.get("quality")])
                                                                            else:
                                                                                format_mismatch='True'
                                                                                format_mismatch_.append([mm.get("link"),"worng format:",hh.get("quality")])   
                                                               # for kk in range (g,len(mm.get("formats"))):
                                                                if hh.get("price")=='0.0':
                                                                    price='0.00'
                                                                if price=='0.00':                                         
                                                                    if mm.get("formats")[kk].get("price")==price:       
                                                                        price_match='True'
                                                                        price_match_=['True']
                                                                  
                                                                    else:
                                                                        price_mismatch='True'
                                                                        price_mismatch_.append([mm.get("link"),"worng price:",hh.get("price")])

                                                                else:
                                                                    if mm.get("formats")[kk].get("price")==hh.get("price"):   
                                                                        price_match='True'
                                                                        price_match_=['True']
                                                                  
                                                                    else:
                                                                        price_mismatch='True'
                                                                        price_mismatch_.append([mm.get("link"),"worng price:",hh.get("price")])
                                                                if mm.get("formats")[kk].get("type")=='purchase':
                                                                    purchase_type='buy'
                                                                else:
                                                                    purchase_type='rent' 
                                                                if purchase_type==hh.get("purchase_type"):
                                                                    purchase_type_match='True'
                                                                    purchase_type_match_=['True']
                                                                    g=g+1
                                                                    break
                                                                else:
                                                                    purchase_type_mismatch='True'
                                                                    purchase_type_mismatch_.append([mm.get("link"),"worng purchase_type:",hh.get("purchase_type")])
                                                                    g=g+1
                                                                    break

                                             
                                        else:
                                            print("No web sources")
                                            link_mismatch='True'
                                            link_mismatch_.append(jj)
                          
                        if data_resp_gb.get("subscription_web_sources")!=[]:
                            a=data_resp_gb.get("subscription_web_sources")
  #                          import pdb;pdb.set_trace()
                            if a:
                                for x in a:
                                    b=(x.get("link")).encode("utf-8")
                                    c=(x.get("source")).encode("utf-8") 
        	                    if 'amazon_prime' in c or 'amazon_buy' in c:
            	                        c='amazon'
    		                    if 'netflix' in c:
                                        c='netflixusa'
        		            if c=='hbo':
                                        c='hbogo'
                                    if c=='hbo_now':
                                        c='hbonow'
        		            if c=='google_play':
                                        c='googleplay'
        		            if c=='hulu_plus':
            		                c='hulu'
            		            if c =='verizon_on_demand':
                                        c='verizon'
            		   	    if c=='showtime_subscription':
                                        c='showtime'
                                    if 'vuduapp' in b:
                                        subscription_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                    if 'hbo.hbonow' in b or 'hbonow://asset' in b:
                                        subscription_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                    if 'play.google' in b:
                                        try:
                                            subscription_source_program_id=re.findall("\w+-\w+.*?",b)[0]
                                        except IndexError:
                                            subscription_source_program_id=re.findall("\w+.*?", b)[-1:][0]
 
           	                    if 'aiv://aiv/play' in b:
             	                        subscription_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                     
                                    if '//itunes.apple.com/us/movie' in b:
                                        subscription_source_program_id=re.findall("\d+", b)[0]
                                    if '//www.amazon.com/gp' in b:
                                        subscription_source_program_id=re.findall("\w+\d+\w+", b)[0]
                                    if '//click.linksynergy.com/' in b:
                     	                subscription_source_program_id=re.findall("\d+", b)[-1:][0]
            	                    if '//www.youtube.com/w' in b:
                                        subscription_source_program_id=re.findall("\w+\d+\w+", b)[0]
                                    if '//www.verizon.com/Ondemand/' in b:
                                        subscription_source_program_id=re.findall("\w+\d+\w+", b)[0]
                                    if '//play.hbonow.com/feature/' in b:
                                        a10=re.findall("\w+.*?", b)
                                        subscription_source_program_id=':'.join(map(str, [a10[i] for i in range(5,9)]))
                                    if 'netflix.com' in b:
                                        subscription_source_program_id=re.findall("\w+.*?",b)[-1:][0] 
                                    if 'http://www.showtime.com/#' in b:
                                        subscription_source_program_id=re.findall("\w+.*?",b)[-1:][0]
                                    if 'http://www.hulu.com/watch' in b:
                                        try:
                                            a14=re.findall("\w+.*?",b)[-5:]
            		                    subscription_source_program_id='-'.join(map(str,[a14[i] for i in range(0,len(a14))]))
                                        except IndexError:
                                            subscription_source_program_id=re.findall("\w+.*?",b)[-1:][0]
                                    if '/play.hbonow.com/episode/' in b:
                                        try: 
            		                    subscription_source_program_id=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                        except IndexError:
                                            a15=re.findall("\w+.*?.-*?", b)
                                            subscription_source_program_id=''.join(map(str, [a15[i] for i in range(5,len(a15))]))

                                    for hh in projectx_videos:
                                        if hh.get("fetched_from")=='GuideBox' and hh.get("link").get("uri").encode()==b:
                                            try:
                                                if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("constraints") is not None:
                                                    subscription_link_match='True'
                                                elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("constraints") is not None:
                                                    subscription_link_match='True'
                                                else:    
                                                    subscription_link_mismatch='True'
                                                    subscription_link_mismatch_.append(hh.get("link").get("uri").encode()) 
                                                  
                                                if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("source_program_id").encode()==subscription_source_program_id and hh.get("constraints") is not None:
                                                    subscription_source_program_id_match="True"
                                                elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("source_program_id").encode()==subscription_source_program_id and hh.get("constraints") is not None:
                                                    subscription_source_program_id_match="True"
                                                else:
                                                    subscription_source_program_id_mismatch='True'
                                                    subscription_source_program_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_source_program_id:",hh.get("source_program_id")])

                                                if hh.get("launch_id") is not None:
                                                    if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("launch_id").encode()==subscription_source_program_id and hh.get("constraints") is not None:
                                                        subscription_launch_id_match="True"
                                                    elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and hh.get("launch_id").encode()==subscription_source_program_id and hh.get("constraints") is not None:
                                                        subscription_launch_id_match="True"
                                                    else:
                                                        subscription_launch_id_mismatch='True'
                                                        subscription_launch_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_launch_id:",hh.get("launch_id")])
                                                if hh.get("roku_id") is not None:
                                                    if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("roku_id") and hh.get("constraints") is not None:
                                                        subscription_roku_id_match="True"
                                                    elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("roku_id") and hh.get("constraints") is not None:
                                                        subscription_roku_id_match="True"
                                                    else:
                                                        subscription_roku_id_mismatch='True'
                                                        subscription_roku_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_roku_id:",hh.get("roku_id")])

                                                if hh.get("firetv_id") is not None:

                                                    if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("firetv_id") and hh.get("constraints") is not None:
                                                        subscription_firetv_id_match="True"
                                                    elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("firetv_id") and hh.get("constraints") is not None:
                                                        subscription_firetv_id_match="True"
                                                    else:
                                                        subscription_firetv_id_mismatch='True'
                                                        subscription_firetv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_firetv_id:",hh.get("firetv_id")])

 
                                                if hh.get("appletv_id") is not None:                                          
 
                                                    if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("appletv_id") and hh.get("constraints") is not None:
                                                        subscription_appletv_id_match="True"
                                                    elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("appletv_id") and hh.get("constraints") is not None:
                                                        subscription_appletv_id_match="True"
                                                    else:
                                                        subscription_appletv_id_mismatch='True'
                                                        subscription_appletv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_appletv_id:",hh.get("appletv_id")])
                                                if hh.get("androidtv_id") is not None:

                                                    if ("service_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("androidtv_id") and hh.get("constraints") is not None:
                                                        subscription_androidtv_id_match="True"
                                                    elif ("addon_subscription" in hh.get("constraints") or hh.get("constraints")==[]) and subscription_source_program_id in hh.get("amdroidtv_id") and hh.get("constraints") is not None:
                                                        subscription_androidtv_id_match="True"
                                                    else:
                                                        subscription_androidtv_id_mismatch='True'
                                                        subscription_androidtv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_androidtv_id:",hh.get("androidtv_id")]) 
                                            except TypeError:
                                                pass            

    	                if data_resp_gb.get("tv_everywhere_web_sources")!=[]:
                            a=data_resp_gb.get("tv_everywhere_web_sources")
   #                         import pdb;pdb.set_trace()
                            if a:
                                for x in a:
                                    b=(x.get("link")).encode("utf-8")
                                    c=(x.get("source")).encode("utf-8")
         	                    if 'amazon_prime' in c or 'amazon_buy' in c:
                                        c='amazon'
                                    if 'netflix' in c:
                                        c='netflixusa'
                		    if c=='hbo':
                                        c='hbogo'
                                    if c=='hbo_now':
                                        c='hbonow'
                    		    if c=='starz_tveverywhere':
                    		        c='starz'
                	            if "hbogo://deeplink/" in b or 'hbonow://asset' in b: #"""****************"""
                                        tv_everywhere_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                 	            if "starz://play" in b or "//www.starz.com/" in b:
                                        tv_everywhere_source_program_id=re.findall("\w+.*?", b)[-1:][0]
                                    if "//play.hbogo.com/feature" in b:
                                        try:
                                            tv_everywhere_source_program_id=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                        except IndexError:
                                            try:
                                                a3=re.findall("\w+.*?", b)
                                                tv_everywhere_source_program_id=':'.join(map(str, [a3[i] for i in range(5,9)]))
                                            except IndexError:
                                                tv_everywhere_source_program_id=re.findall("\w+.*?",b)[-1:][0]
                                    if 'http://www.showtimeanytime.com/#' in b:
                                        tv_everywhere_source_program_id=re.findall("\d+\w+", b)[0]
                                    if '/play.hbogo.com/episode/' in b:
                                        try:
                                            a6=re.findall("\w+.*?.-*?", b)
                                            tv_everywhere_source_program_id=''.join(map(str, [a6[i] for i in range(5,len(a6))]))
                                        except IndexError:
                 	   	            try:
                		                tv_everywhere_source_program_id=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                            except IndexError:
                        	                tv_everywhere_source_program_id=re.findall("\w+.*?",b)[-1:][0]
                                    for hh in projectx_videos:
#                                        import pdb;pdb.set_trace()
                                        try:
                                            if hh.get("fetched_from")=='GuideBox' and "cable_subscription" in hh.get("constraints"):
                                                if hh.get("link").get("uri").encode()==b:
                                                    tv_everywhere_link_match='True'
                                                else:
                                                    tv_everywhere_link_mismatch='True'
                                                    tv_everywhere_link_mismatch_.append(hh.get("link").get("uri").encode())
                                                if hh.get("link").get("uri").encode()==b and hh.get("source_program_id").encode()==tv_everywhere_source_program_id:
                                                    tv_everywhere_source_program_id_match ='True'
                                                else: 
                                                    tv_everywhere_source_program_id_mismatch='True'
                                                    tv_everywhere_source_program_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_source_program_id:",hh.get("source_program_id")])  

                                                if hh.get("launch_id") is not None:

                                                    if hh.get("link").get("uri").encode()==b and hh.get("launch_id").encode()==tv_everywhere_source_program_id:
                                                        tv_everywhere_launch_id_match ='True'
                                                    else:
                                                        tv_everywhere_launch_id_mismatch='True'
                                                        tv_everywhere_launch_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_launch_id:",hh.get("launch_id")])

                                                if hh.get("roku_id") is not None: 
                                                    if hh.get("link").get("uri").encode()==b and tv_everywhere_source_program_id in hh.get("roku_id"):
                                                        tv_everywhere_roku_id_match ='True'
                                                    else:
                                                        tv_everywhere_roku_id_mismatch='True'
                                                        tv_everywhere_roku_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_roku_id:",hh.get("roku_id")]) 


                                                if hh.get("firetv_id") is not None:
                                                    if hh.get("link").get("uri").encode()==b and tv_everywhere_source_program_id in hh.get("firetv_id"):
                                                        tv_everywhere_firetv_id_match ='True'
                                                    else:
                                                        tv_everywhere_firetv_id_mismatch='True'
                                                        tv_everywhere_firetv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_firetv_id:",hh.get("firetv_id")])
        
                                                if hh.get("appletv_id") is not None:
                                                    if hh.get("link").get("uri").encode()==b and tv_everywhere_source_program_id in hh.get("appletv_id"):
                                                        tv_everywhere_appletv_id_match ='True'
                                                    else:
                                                        tv_everywhere_appletv_id_mismatch='True'
                                                        tv_everywhere_appletv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_appletv_id:",hh.get("appletv_id")])
 
                                                if hh.get("androidtv_id") is not None:
                                                    if hh.get("link").get("uri").encode()==b and tv_everywhere_source_program_id in hh.get("androidtv_id"):
                                                        tv_everywhere_androidtv_id_match ='True'
                                                    else:
                                                        tv_everywhere_androidtv_id_mismatch='True'
                                                        tv_everywhere_androidtv_id_mismatch_.append(["Service:", c,hh.get("link").get("uri"),"Wrong_androidtv_id:",hh.get("androidtv_id")])
                                        except TypeError:
                                            pass    
                                        
             
     	                if link_mismatch=='True' or purchase_source_program_id_mismatch=='True' or format_mismatch=='True' or price_mismatch=='Truee' or purchase_type_mismatch=='True' or subscription_link_mismatch=='True' or tv_everywhere_link_mismatch=='True' or tv_everywhere_source_program_id_mismatch=='True' or subscription_source_program_id_mismatch=='True' or tv_everywhere_androidtv_id_mismatch=='True' or tv_everywhere_appletv_id_mismatch=='True' or tv_everywhere_firetv_id_mismatch=='True' or tv_everywhere_roku_id_mismatch=='True' or tv_everywhere_launch_id_mismatch=='True' or subscription_androidtv_id_mismatch=='True' or subscription_appletv_id_mismatch=='True' or subscription_firetv_id_mismatch=='True' or subscription_roku_id_mismatch=='True' or subscription_launch_id_mismatch=='True' or purchase_androidtv_id_mismatch=='True' or purchase_appletv_id_mismatch=='True' or purchase_firetv_id_mismatch=='True' or purchase_roku_id_mismatch=='True' or purchase_launch_id_mismatch=='True':
                            v=v+1
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"show_type":show_type,"projectx_id_gb":str(projectx_id_gb),"link_mismatch":link_mismatch,"Wrong link":link_mismatch_,"purchase_source_program_id_mismatch":purchase_source_program_id_mismatch,"Wrong_source_program_id":purchase_source_program_id_mismatch_,"format_mismatch":format_mismatch,"Wrong format":format_mismatch_,"price_mismatch":price_mismatch,"Wrong price":price_mismatch_,"purchase_type_mismatch":purchase_type_mismatch,"wrong purchase type":purchase_type_mismatch_,"purchase_launch_id_mismatch":purchase_launch_id_mismatch,"wrong purchase_launch_id":purchase_launch_id_mismatch_,"purchase_roku_id_mismatch":purchase_roku_id_mismatch,"wrong purchase_roku_id":purchase_roku_id_mismatch_,"purchase_firetv_id_mismatch":purchase_firetv_id_mismatch,"wrong purchase_firetv_id":purchase_firetv_id_mismatch_,"purchase_appletv_id_mismatch":purchase_appletv_id_mismatch,"wrong purchase_appletv_id":purchase_appletv_id_mismatch_,"purchase_androidtv_id_mismatch":purchase_androidtv_id_mismatch,"wrong purchase_androidtv_id":purchase_androidtv_id_mismatch_,"subscription_link_mismatch":subscription_link_mismatch,"Wrong subscription_link":subscription_link_mismatch_,"tv_everywhere_link_mismatch":tv_everywhere_link_mismatch,"Wrong tv_everywhere_link":tv_everywhere_link_mismatch_,"tv_everywhere_source_program_id_mismatch":tv_everywhere_source_program_id_mismatch,"Wrong tv_everywhere_source_program_id":tv_everywhere_source_program_id_mismatch_,"tv_everywhere_launch_id_mismatch":tv_everywhere_launch_id_mismatch,"Wrong tv_everywhere_launch_id":tv_everywhere_launch_id_mismatch_,"tv_everywhere_roku_id_mismatch":tv_everywhere_roku_id_mismatch,"Wrong tv_everywhere_roku_id":tv_everywhere_roku_id_mismatch_,"tv_everywhere_firetv_id_mismatch":tv_everywhere_firetv_id_mismatch,"Wrong tv_everywhere_firetv_id":tv_everywhere_firetv_id_mismatch_,"tv_everywhere_appletv_id_mismatch":tv_everywhere_appletv_id_mismatch,"Wrong tv_everywhere_appletv_id":tv_everywhere_appletv_id_mismatch_,"tv_everywhere_androidtv_id_mismatch":tv_everywhere_androidtv_id_mismatch,"Wrong tv_everywhere_androidtv_id":tv_everywhere_androidtv_id_mismatch_,"subscription_source_program_id_mismatch":subscription_source_program_id_mismatch,"Wrong subscription_source_program_id":subscription_source_program_id_mismatch_,"subscription_launch_id_mismatch":subscription_launch_id_mismatch,"wrong subscription_launch_id":subscription_launch_id_mismatch_,"subscription_roku_id_mismatch":subscription_roku_id_mismatch,"wrong subscription_roku_id":subscription_roku_id_mismatch_,"subscription_firetv_id_mismatch":subscription_firetv_id_mismatch,"wrong subscription_firetv_id":subscription_firetv_id_mismatch_,"subscription_appletv_id_mismatch":subscription_appletv_id_mismatch,"wrong subscription_appletv_id":subscription_appletv_id_mismatch_,"subscription_androidtv_id_mismatch":subscription_androidtv_id_mismatch,"wrong subscription_androidtv_id":subscription_androidtv_id_mismatch_,"Result for Ott_link population":"Fail"})
                        else:
                            t=t+1
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"show_type":show_type,"projectx_id_gb":str(projectx_id_gb),"link_mismatch":link_mismatch,"Wrong link":link_mismatch_,"purchase_source_program_id_mismatch":purchase_source_program_id_mismatch,"Wrong_source_program_id":purchase_source_program_id_mismatch_,"format_mismatch":format_mismatch,"Wrong format":format_mismatch_,"price_mismatch":price_mismatch,"Wrong price":price_mismatch_,"purchase_type_mismatch":purchase_type_mismatch,"wrong purchase type":purchase_type_mismatch_,"purchase_launch_id_mismatch":purchase_launch_id_mismatch,"wrong purchase_launch_id":purchase_launch_id_mismatch_,"purchase_roku_id_mismatch":purchase_roku_id_mismatch,"wrong purchase_roku_id":purchase_roku_id_mismatch_,"purchase_firetv_id_mismatch":purchase_firetv_id_mismatch,"wrong purchase_firetv_id":purchase_firetv_id_mismatch_,"purchase_appletv_id_mismatch":purchase_appletv_id_mismatch,"wrong purchase_appletv_id":purchase_appletv_id_mismatch_,"purchase_androidtv_id_mismatch":purchase_androidtv_id_mismatch,"wrong purchase_androidtv_id":purchase_androidtv_id_mismatch_,"subscription_link_mismatch":subscription_link_mismatch,"Wrong subscription_link":subscription_link_mismatch_,"tv_everywhere_link_mismatch":tv_everywhere_link_mismatch,"Wrong tv_everywhere_link":tv_everywhere_link_mismatch_,"tv_everywhere_source_program_id_mismatch":tv_everywhere_source_program_id_mismatch,"Wrong tv_everywhere_source_program_id":tv_everywhere_source_program_id_mismatch_,"tv_everywhere_launch_id_mismatch":tv_everywhere_launch_id_mismatch,"Wrong tv_everywhere_launch_id":tv_everywhere_launch_id_mismatch_,"tv_everywhere_roku_id_mismatch":tv_everywhere_roku_id_mismatch,"Wrong tv_everywhere_roku_id":tv_everywhere_roku_id_mismatch_,"tv_everywhere_firetv_id_mismatch":tv_everywhere_firetv_id_mismatch,"Wrong tv_everywhere_firetv_id":tv_everywhere_firetv_id_mismatch_,"tv_everywhere_appletv_id_mismatch":tv_everywhere_appletv_id_mismatch,"Wrong tv_everywhere_appletv_id":tv_everywhere_appletv_id_mismatch_,"tv_everywhere_androidtv_id_mismatch":tv_everywhere_androidtv_id_mismatch,"Wrong tv_everywhere_androidtv_id":tv_everywhere_androidtv_id_mismatch_,"subscription_source_program_id_mismatch":subscription_source_program_id_mismatch,"Wrong subscription_source_program_id":subscription_source_program_id_mismatch_,"subscription_launch_id_mismatch":subscription_launch_id_mismatch,"wrong subscription_launch_id":subscription_launch_id_mismatch_,"subscription_roku_id_mismatch":subscription_roku_id_mismatch,"wrong subscription_roku_id":subscription_roku_id_mismatch_,"subscription_firetv_id_mismatch":subscription_firetv_id_mismatch,"wrong subscription_firetv_id":subscription_firetv_id_mismatch_,"subscription_appletv_id_mismatch":subscription_appletv_id_mismatch,"wrong subscription_appletv_id":subscription_appletv_id_mismatch_,"subscription_androidtv_id_mismatch":subscription_androidtv_id_mismatch,"wrong subscription_androidtv_id":subscription_androidtv_id_mismatch_,"Result for Ott_link population":"Pass"})  


                        print datetime.datetime.now()                       
                        print("\n")
                        print(result_sheet,"thread_name:", name, "total count of GB ID: ", total ,"Not ingested Gb_id :", w, "Ott_link pass: ",t,"missing ott_contentsand additional/duplicates ott_contents :", v, "missing ott_contents: ", v, "Total fail : ",v, "Total pass: ", t)


            except (ValueError,TypeError,AttributeError, pymongo.errors.OperationFailure,_mysql_exceptions.OperationalError,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError) as e:
                print ("caught exception ......................................................................................", e)
                continue

            print(result_sheet,"thread_name:", name, "total count of GB ID: ", total ,"Not ingested Gb_id :", w, "Ott_link pass: ",t, "Duplicates/Additinal ott_contents :",h, "missing ott_contentsand additional/duplicates ott_contents :", v, "missing ott_contents: ", m, "Total fail : ",h+v+m, "Total pass: ", t)
            print("\n")
            print datetime.datetime.now()
            print("\n")
            print("gb_ids not ingested",GB_id, len(GB_id))
            print("\n") 
            print("gb_ids MO not ingested",GB_Id, len(GB_Id))
            print("\n")
            print("gb_id_sm not id DB",Gb_sm_id, len(Gb_sm_id))
            print("\n")
            print(datetime.datetime.now())


    connection.close()


t1=threading.Thread(target=ott,args=(0,"starting thread-1",30000,1))
t1.start()
t2 =threading.Thread(target=ott,args=(30001,'starting thread-2',60000,2))
t2.start()
t3 = threading.Thread(target=ott,args=(60001,'starting thread-3',90000,3))
t3.start()
t4 = threading.Thread(target=ott,args=(90001,'starting thread-4',120000,4))
t4.start()
t5 = threading.Thread(target=ott,args=(120001,'starting thread-5',150000,5))
t5.start()
t6 = threading.Thread(target=ott,args=(150001,'starting thread-6',180000,6))
t6.start()
t7 =threading.Thread(target=ott,args=(180001,'starting thread-7',210000,7))
t7.start()
t8 = threading.Thread(target=ott,args=(210001,'starting thread-8',240000,8))
t8.start()
t9 = threading.Thread(target=ott,args=(240001,'starting thread-9',270000,9))
t9.start()
t10= threading.Thread(target=ott,args=(270001,'starting thread-10',300000,10))
t10.start()
t11=threading.Thread(target=ott,args=(300001,'starting thread-11',330000,11))
t11.start()
t12=threading.Thread(target=ott,args=(330001,'starting thread-12',360000,12))
t12.start()
t13=threading.Thread(target=ott,args=(360001,'starting thread-13',390000,13))
t13.start()
t14=threading.Thread(target=ott,args=(390001,'starting thread-14',420000,14))
t14.start()
t15=threading.Thread(target=ott,args=(420001,'starting thread-15',450000,15))
t15.start()
t16=threading.Thread(target=ott,args=(450001,'starting thread-16',480000,16))
t16.start()
t17=threading.Thread(target=ott,args=(480001,'starting thread-17',510000,17))
t17.start()
t18=threading.Thread(target=ott,args=(510001,'starting thread-18',540000,18))
t18.start()
t19=threading.Thread(target=ott,args=(540001,'starting thread-19',570000,19))
t19.start()
t20=threading.Thread(target=ott,args=(570001,'starting thread-20',600000,20))
t20.start()
t21=threading.Thread(target=ott,args=(600001,'starting thread-21',700000,21))
t21.start()




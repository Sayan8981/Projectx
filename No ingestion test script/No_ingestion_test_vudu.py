"""Writer: Saayan"""

import threading
import MySQLdb
import collections
from pprint import pprint
import sys
import csv
import os
import pymysql
import pymongo
import datetime
import sys
import urllib2
import json
import os
from urllib2 import HTTPError
import httplib
import socket

def ingestion(start,name,end,id):
    
    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["qadb"]
    mytable=mydb["vududump"]

    result_sheet='/No_ingestion_test_vudu%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["vudu_id MO","vudu_id SE","vudu_id SM","title","release_year","series_title","episode_number","projectx_id_vudu","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        total=0 
        total1=0
        total2=0
        p=0
        q=0
        r=0
        s=0
	t=0
	u=0
        x=0
	y=0
	z=0
        for aa in range(start,end,50):
	    query1=mytable.aggregate([{"$skip":aa},{"$limit":50},{"$match":{"show_type":{"$in":["MO","SE","SM"]}}},{"$project":{"launch_id":1,"_id":0,"show_type":1,"title":1,"release_year":1,"series_title":1,"episode_number":1}}])
            for i in query1:
                if i.get("show_type")=='SE':
                    vudu_id_se=i.get("launch_id").encode()
                    vudu_projectx_id_se=[] 
                    total=total+1
	            try:
                        url_vudu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=Showtime&showType=SE" %vudu_id_se
                        response_vudu=urllib2.Request(url_vudu)
                        response_vudu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_vudu=urllib2.urlopen(response_vudu)
                        data_vudu=resp_vudu.read()
                        data_resp_vudu=json.loads(data_vudu)
                    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                        continue
            
                    for jj in data_resp_vudu:
                        if jj["data_source"]=="vudu" and jj["type"]=="Program" and jj["sub_type"]=="SE":
                            vudu_projectx_id_se.append(jj["projectx_id"])
                    if vudu_projectx_id_se:
                        if len(vudu_projectx_id_se)>1:
                            q=q+1
                            writer.writerow({"vudu_id SE":str(vudu_id_se),"title":i.get("title").encode('ascii','ignore'),"release_year":i.get("release_year"),"series_title":i.get("series_title").encode('ascii','ignore'),"episode_number":i.get("episode_number"),"projectx_id_vudu":vudu_projectx_id_se,"Comment":'Multiple ingestion for same content of vudu'})
                        if len(vudu_projectx_id_se)==1:
                            r=r+1
                    else:
                        p=p+1
                        writer.writerow({"vudu_id SE":str(vudu_id_se),"title":i.get("title").encode('ascii','ignore'),"release_year":i.get("release_year"),"series_title":i.get("series_title").encode('ascii','ignore'),"episode_number":i.get("episode_number"),"projectx_id_vudu":'',"Comment":'Not Ingested'})   
                    print("thread name:", name,"total vudu id SE :", total ,"Not ingested: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
    
                if i.get("show_type")=='MO':
                    vudu_id_mo=i.get("launch_id").encode()
                    vudu_projectx_id_mo=[]
                    total1=total1+1
                    try:
                        url_vudu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=Showtime&showType=SE" %vudu_id_mo
                        response_vudu=urllib2.Request(url_vudu)
                        response_vudu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_vudu=urllib2.urlopen(response_vudu)
                        data_vudu=resp_vudu.read()
                        data_resp_vudu=json.loads(data_vudu)
                    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                        continue

                    for jj in data_resp_vudu:
                        if jj["data_source"]=="vudu" and jj["type"]=="Program" and jj["sub_type"]=="MO":
                            vudu_projectx_id_mo.append(jj["projectx_id"])
                    if vudu_projectx_id_mo:
                        if len(vudu_projectx_id_mo)>1:
                            t=t+1
                            writer.writerow({"vudu_id MO":str(vudu_id_mo),"title":i.get("title").encode('ascii','ignore'),"release_year":i.get("release_year"),"series_title":'',"projectx_id_vudu":vudu_projectx_id_mo,"Comment":'Multiple ingestion for same content of vudu'})
                        if len(vudu_projectx_id_mo)==1:
                            u=u+1
                    else:
                        s=s+1
                        writer.writerow({"vudu_id MO":str(vudu_id_mo),"title":i.get("title").encode('ascii','ignore'),"release_year":i.get("release_year"),"series_title":'',"projectx_id_vudu":'',"Comment":'Not Ingested'})
                    print("thread name:", name,"total vudu id MO :", total1 ,"Not ingested: ", s, "Multiple mapped content :", t, "Total Fail: ", s+t, "Pass: ", u)
                if i.get("show_type")=='SM':
                    vudu_id_sm=i.get("launch_id").encode()
                    vudu_projectx_id_sm=[]
                    total2=total2+1
                    try:
                        url_vudu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=Showtime&showType=SE" %vudu_id_sm
                        response_vudu=urllib2.Request(url_vudu)
                        response_vudu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_vudu=urllib2.urlopen(response_vudu)
                        data_vudu=resp_vudu.read()
                        data_resp_vudu=json.loads(data_vudu)
                    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                        continue

                    for jj in data_resp_vudu:
                        if jj["data_source"]=="vudu" and jj["type"]=="Program" and jj["sub_type"]=="SM":
                            vudu_projectx_id_sm.append(jj["projectx_id"])
                    if vudu_projectx_id_sm:
                        if len(vudu_projectx_id_sm)>1:
                            y=y+1
                            writer.writerow({"vudu_id SM":str(vudu_id_sm),"title":i.get("title").encode('ascii','ignore'),"release_year":i.get("release_year"),"series_title":'',"projectx_id_vudu":vudu_projectx_id_sm,"Comment":'Multiple ingestion for same content of vudu'})
                        if len(vudu_projectx_id_sm)==1:
                            z=z+1
                    else:
                        x=x+1
                        writer.writerow({"vudu_id SM":str(vudu_id_sm),"title":i.get("title").encode('ascii','ignore'),"release_year":i.get("release_year"),"series_title":'',"projectx_id_vudu":'',"Comment":'Not Ingested'})
                    print("thread name:", name,"total vudu id MO :", total2 ,"Not ingested: ", x, "Multiple mapped content :", y, "Total Fail: ", x+y, "Pass: ", z)
        print("total vudu id MO :", total1 ,"Not ingested: ",s,  "Multiple mapped content :", t, "Total Fail: ", s+t, "Pass: ", u)
        print("total vudu id SE :", total ,"Not ingested: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
        print("total vudu id SM :", total2 ,"Not ingested: ", x, "Multiple mapped content :", y, "Total Fail: ", x+y, "Pass: ", z)
    print(datetime.datetime.now()) 

t1=threading.Thread(target=ingestion,args=(0,"thread-1",100,1))
t1.start()
"""t2=threading.Thread(target=ingestion,args=(20001,"thread-2",40001,2))
t2.start()
t3=threading.Thread(target=ingestion,args=(40001,"thread-3",60001,3))
t3.start()
t4=threading.Thread(target=ingestion,args=(60001,"thread-4",88001,4))
t4.start()"""


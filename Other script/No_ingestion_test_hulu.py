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

def ingestion_se(start,name,end,id):
    
    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["qadb"]
    mytable=mydb["HuluValidEpisodes"]

    result_sheet='/No_ingestion_test_hulu%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["hulu_id MO","hulu_id SE","hulu_id SM","title","programming_type","type","site_id","series","projectx_id_hulu","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        total=0 
        total1=0
        total2=0
        p=0
        q=0
        r=0

        for aa in range(start,end,1000):
	    query1=mytable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"programming_type":"Full Episode"}},{"$project":{"id":1,"_id":0,"title":1,"programming_type":1,"type":1,"site_id":1,"series":1}}])
	    arr_hulu_se=[]
            for i in query1:
                hulu_id_se=i.get("id")
                print hulu_id_se
	        arr_hulu_se.append(hulu_id_se)
	        print len(arr_hulu_se)

                hulu_projectx_id_se=[] 
                total1=total1+1
	        try:
                    url_hulu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Showtime&showType=SE" %hulu_id_se
                    response_hulu=urllib2.Request(url_hulu)
                    response_hulu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_hulu=urllib2.urlopen(response_hulu)
                    data_hulu=resp_hulu.read()
                    data_resp_hulu=json.loads(data_hulu)
                except httplib.BadStatusLine:
                    continue
                except urllib2.HTTPError:
                    continue
                except socket.error:
                    continue
            
                for jj in data_resp_hulu:
                    if jj["data_source"]=="hulu" and jj["type"]=="Program" and jj["sub_type"]=="SE":
                        hulu_projectx_id_se.append(jj["projectx_id"])
                if hulu_projectx_id_se:
                    if len(hulu_projectx_id_se)>1:
                        q=q+1
                        writer.writerow({"hulu_id SE":hulu_id_se,"title":i.get("title").encode('ascii','ignore'),"programming_type":i.get("programming_type").encode('ascii','ignore'),"type":i.get("type").encode('ascii','ignore'),"site_id":i.get("site_id"),"series":i.get("series").get("name").encode('ascii','ignore'),"projectx_id_hulu":hulu_projectx_id_se,"Comment":'Multiple ingestion for same content of hulu'})
                    if len(hulu_projectx_id_se)==1:
                        r=r+1
                else:
                    p=p+1
                    writer.writerow({"hulu_id SE":hulu_id_se,"title":i.get("title").encode('ascii','ignore'),"programming_type":i.get("programming_type").encode('ascii','ignore'),"type":i.get("type").encode('ascii','ignore'),"site_id":i.get("site_id"),"series":i.get("series").get("name").encode('ascii','ignore'),"projectx_id_hulu":'',"Comment":'Not Ingested'})   
                print("thread name:", name,"total hulu id SE :", total1 ,"Not ingested: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
        print("total hulu id SE :", total1 ,"Not ingested: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
    
    print(datetime.datetime.now()) 


def ingestion_mo(start,name,end,id):
    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["qadb"]
    mytable1=mydb["HuluValidMovies"]

    result_sheet='/No_ingestion_test_hulu%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["hulu_id MO","hulu_id SE","hulu_id SM","title","programming_type","type","site_id","projectx_id_hulu","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        total=0
        total1=0
        total2=0
        p=0
        q=0
        r=0

        for aa in range(start,end,1000):
            query2=mytable1.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"programming_type":"Full Movie"}},{"$project":{"id":1,"_id":0,"title":1,"programming_type":1,"type":1,"site_id":1,"series":1}}])
	    arr_hulu_mo=[]

            for i in query2:
                hulu_id_mo=i.get("id")
                arr_hulu_mo.append(hulu_id_mo)
                print len(arr_hulu_mo)

            #for ee in arr_hulu_mo:
                hulu_projectx_id_mo=[]
                total1=total1+1
                try:
                    url_hulu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Showtime&showType=SE" %hulu_id_mo
                    response_hulu=urllib2.Request(url_hulu)
                    response_hulu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_hulu=urllib2.urlopen(response_hulu)
                    data_hulu=resp_hulu.read()
                    data_resp_hulu=json.loads(data_hulu)
                except httplib.BadStatusLine:
                    continue
                except urllib2.HTTPError:
                    continue
                except socket.error:
                    continue

                for jj in data_resp_hulu:
                    if jj["data_source"]=="hulu" and jj["type"]=="Program" and jj["sub_type"]=="MO":
                        hulu_projectx_id_mo.append(jj["projectx_id"])
                if hulu_projectx_id_mo:
                    if len(hulu_projectx_id_mo)>1:
                        q=q+1
                        writer.writerow({"hulu_id MO":hulu_id_mo,"title":i.get("title").encode('ascii','ignore'),"programming_type":i.get("programming_type").encode('ascii','ignore'),"type":i.get("type").encode('ascii','ignore'),"site_id":i.get("site_id"),"projectx_id_hulu":hulu_projectx_id_mo,"Comment":'Multiple ingestion for same content of hulu'})
                    if len(hulu_projectx_id_mo)==1:
                        r=r+1
                else:
                    p=p+1
                    writer.writerow({"hulu_id MO":hulu_id_mo,"title":i.get("title").encode('ascii','ignore'),"programming_type":i.get("programming_type").encode('ascii','ignore'),"type":i.get("type").encode('ascii','ignore'),"site_id":i.get("site_id"),"projectx_id_hulu":'',"Comment":'Not Ingested'})
                print("thread name:", name,"total hulu id MO :", total1 ,"Not ingested: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
        print("total hulu id MO :", total1 ,"Not ingested: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)

    print(datetime.datetime.now()) 

t1=threading.Thread(target=ingestion_se,args=(0,"thread-1",20001,1))
t1.start()
t5=threading.Thread(target=ingestion_mo,args=(0,"thread-5",2001,5))
t5.start()
t2=threading.Thread(target=ingestion_se,args=(20001,"thread-2",40001,2))
t2.start()
t3=threading.Thread(target=ingestion_se,args=(40001,"thread-3",60001,3))
t3.start()
t4=threading.Thread(target=ingestion_se,args=(60001,"thread-4",88001,4))
t4.start()



"""Writer: Saayan"""

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

def ingestion():
    
    conn1=pymysql.connect(user="root",passwd="branch@123",host="localhost",db="branch_service")
    cur1=conn1.cursor()

    result_sheet='/No_ingestion_test_showtime.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["showtime_id MO","showtime_id SE","showtime_id SM","projectx_id_showtime","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        total=0 
        total1=0
        total2=0
	query1="select source_program_id,item_type from showtime_programs where expired=0;"
	cur1.execute(query1)
	res1=cur1.fetchall()
        j=0
        k=0
        l=0
        m=0
        n=0
        o=0
        p=0
	q=0
	r=0
        for i in res1:
	    print i
	    showtime_projectx_id=[]
            if list(i)!= []:
	        if i[1]=='movie':
		    print total,l,j,k
                    total=total+1
		    try:
		        url_showtime="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Showtime&showType=MO" %i[0]
                        response_showtime=urllib2.Request(url_showtime)
                        response_showtime.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_showtime=urllib2.urlopen(response_showtime)
                        data_showtime=resp_showtime.read()
                        data_resp_showtime=json.loads(data_showtime)
		    except httplib.BadStatusLine:
                        continue
           	    except urllib2.HTTPError:
                	continue
                    except socket.error:
                         continue

                    for jj in data_resp_showtime:
                        if jj["data_source"]=="Showtime" and jj["type"]=="Program" and jj["sub_type"]=="MO":
                            showtime_projectx_id.append(jj["projectx_id"])
                    if len(showtime_projectx_id)>1:
                        j=j+1
                        writer.writerow({"showtime_id MO":str(i[0]),"projectx_id_showtime":showtime_projectx_id,"Comment":'Multiple ingestion for same content of Showtime'})

                    if len(showtime_projectx_id)==1:
                        k=k+1

                    if len(showtime_projectx_id)==0:
                        l=l+1
                        writer.writerow({"showtime_id MO":str(i[0]),"projectx_id_showtime":'',"Comment":'No Ingestion'})
			print("total showtime id MO:", total ,"No ingestion: ", l, "Multiple mapped content :", j, "Total Fail: ", l+j, "Pass: ", k)
                if i[1]=='episode':
                    total1=total1+1
		    try:
                        url_showtime="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Showtime&showType=SE" %i[0]
                        response_showtime=urllib2.Request(url_showtime)
                        response_showtime.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_showtime=urllib2.urlopen(response_showtime)
                        data_showtime=resp_showtime.read()
                        data_resp_showtime=json.loads(data_showtime)
		    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                         continue

                    for jj in data_resp_showtime:
                        if jj["data_source"]=="Showtime" and jj["type"]=="Program" and jj["sub_type"]=="SE":
                            showtime_projectx_id.append(jj["projectx_id"])
                    if len(showtime_projectx_id)>1:
                        q=q+1
                        writer.writerow({"showtime_id SE":str(i[0]),"projectx_id_showtime":showtime_projectx_id,"Comment":'Multiple ingestion for same content of Showtime'})

                    if len(showtime_projectx_id)==1:
                        r=r+1

                    if len(showtime_projectx_id)==0:
                        p=p+1
                        writer.writerow({"showtime_id SE":str(i[0]),"projectx_id_showtime":'',"Comment":'No Ingestion'})   
		    print("total showtime id SE :", total1 ,"No ingestion: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
    		if i[1]=='tv_show':
                    total2=total2+1
		    try:
                        url_showtime="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Showtime&showType=SM" %i[0]
                        response_showtime=urllib2.Request(url_showtime)
                        response_showtime.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_showtime=urllib2.urlopen(response_showtime)
                        data_showtime=resp_showtime.read()
                        data_resp_showtime=json.loads(data_showtime)
		    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                         continue


                    for jj in data_resp_showtime:
                        if jj["data_source"]=="Showtime" and jj["type"]=="Program" and jj["sub_type"]=="SM":
                            showtime_projectx_id.append(jj["projectx_id"])
                    if len(showtime_projectx_id)>1:
                        n=n+1
                        writer.writerow({"showtime_id SM":str(i[0]),"projectx_id_showtime":showtime_projectx_id,"Comment":'Multiple ingestion for same content of Showtime'})

                    if len(showtime_projectx_id)==1:
                        o=o+1

                    if len(showtime_projectx_id)==0:
                        m=m+1
                        writer.writerow({"showtime_id SM":str(i[0]),"projectx_id_showtime":'',"Comment":'No Ingestion'})
                    print("total showtime id SM :", total2 ,"No ingestion: ", m ,"Multiple mapped content :", n, "Total Fail: ", m+n, "Pass: ", o)
        print("total showtime id MO:", total ,"No ingestion: ", l, "Multiple mapped content :", j, "Total Fail: ", l+j, "Pass: ", k)
        print("total showtime id SE :", total1 ,"No ingestion: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
        print("total showtime id SM :", total2 ,"No ingestion: ", m ,"Multiple mapped content :", n, "Total Fail: ", m+n, "Pass: ", o)		
    print("total showtime id :", total+total1+total2 ,"total No ingestion: ", m+p+l, "Multiple mapped content :", q+n+j, "Total Fail: ", m+p+l+q+n+j, "Pass: ", r+o+k)
    
    print(datetime.datetime.now()) 

ingestion()


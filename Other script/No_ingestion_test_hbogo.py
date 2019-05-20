
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

    result_sheet='/No_ingestion_test_hbogo.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["hbogo_id MO","hbogo_id SE","hbogo_id SM","hbogo_id OT","projectx_id_hbogo","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        total=0 
        total1=0
        total2=0
	total3=0
	query1="select launch_id,show_type from hbogo_programs where expired=0"
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
	s=0
	t=0
	u=0
        for i in res1:
	    print i
	    hbogo_projectx_id=[]
            if list(i)!= []:
	        if i[1]=='MO':
		    print total,l,j,k
                    total=total+1
		    try:
		        url_hbogo="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=HBOGO&showType=MO" %i[0]
                        response_hbogo=urllib2.Request(url_hbogo)
                        response_hbogo.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_hbogo=urllib2.urlopen(response_hbogo)
                        data_hbogo=resp_hbogo.read()
                        data_resp_hbogo=json.loads(data_hbogo)
		    except httplib.BadStatusLine:
                        continue
           	    except urllib2.HTTPError:
                	continue
                    except socket.error:
                         continue

                    for jj in data_resp_hbogo:
                        if jj["data_source"]=="HBOGO" and jj["type"]=="Program" and jj["sub_type"]=="MO":
                            hbogo_projectx_id.append(jj["projectx_id"])
                    if len(hbogo_projectx_id)>1:
                        j=j+1
                        writer.writerow({"hbogo_id MO":str(i[0]),"projectx_id_hbogo":hbogo_projectx_id,"Comment":'Multiple ingestion for same content of hbogo'})

                    if len(hbogo_projectx_id)==1:
                        k=k+1

                    if len(hbogo_projectx_id)==0:
                        l=l+1
                        writer.writerow({"hbogo_id MO":str(i[0]),"projectx_id_hbogo":'',"Comment":'No Ingestion'})
			print("total hbogo id MO:", total ,"No ingestion: ", l, "Multiple mapped content :", j, "Total Fail: ", l+j, "Pass: ", k)
                if i[1]=='SE':
                    total1=total1+1
		    try:
                        url_hbogo="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=HBOGO&showType=SE" %i[0]
                        response_hbogo=urllib2.Request(url_hbogo)
                        response_hbogo.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_hbogo=urllib2.urlopen(response_hbogo)
                        data_hbogo=resp_hbogo.read()
                        data_resp_hbogo=json.loads(data_hbogo)
		    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                         continue

                    for jj in data_resp_hbogo:
                        if jj["data_source"]=="HBOGO" and jj["type"]=="Program" and jj["sub_type"]=="SE":
                            hbogo_projectx_id.append(jj["projectx_id"])
                    if len(hbogo_projectx_id)>1:
                        q=q+1
                        writer.writerow({"hbogo_id SE":str(i[0]),"projectx_id_hbogo":hbogo_projectx_id,"Comment":'Multiple ingestion for same content of HBOGO'})

                    if len(hbogo_projectx_id)==1:
                        r=r+1

                    if len(hbogo_projectx_id)==0:
                        p=p+1
                        writer.writerow({"hbogo_id SE":str(i[0]),"projectx_id_hbogo":'',"Comment":'No Ingestion'})   
		    print("total hbogo id SE :", total1 ,"No ingestion: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
    		if i[1]=='SM':
                    total2=total2+1
		    try:
                        url_hbogo="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=HBOGO&showType=SM" %i[0]
                        response_hbogo=urllib2.Request(url_hbogo)
                        response_hbogo.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_hbogo=urllib2.urlopen(response_hbogo)
                        data_hbogo=resp_hbogo.read()
                        data_resp_hbogo=json.loads(data_hbogo)
		    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                         continue


                    for jj in data_resp_hbogo:
                        if jj["data_source"]=="HBOGO" and jj["type"]=="Program" and jj["sub_type"]=="SM":
                            hbogo_projectx_id.append(jj["projectx_id"])
                    if len(hbogo_projectx_id)>1:
                        n=n+1
                        writer.writerow({"hbogo_id SM":str(i[0]),"projectx_id_hbogo":hbogo_projectx_id,"Comment":'Multiple ingestion for same content of HBOGO'})

                    if len(hbogo_projectx_id)==1:
                        o=o+1

                    if len(hbogo_projectx_id)==0:
                        m=m+1
                        writer.writerow({"hbogo_id SM":str(i[0]),"projectx_id_hbogo":'',"Comment":'No Ingestion'})
                    print("total hbogo id SM :", total2 ,"No ingestion: ", m ,"Multiple mapped content :", n, "Total Fail: ", m+n, "Pass: ", o)

                if i[1]=='OT':
                    total3=total3+1
                    try:
                        url_hbogo="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=HBOGO&showType=MO" %i[0]
                        response_hbogo=urllib2.Request(url_hbogo)
                        response_hbogo.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_hbogo=urllib2.urlopen(response_hbogo)
                        data_hbogo=resp_hbogo.read()
                        data_resp_hbogo=json.loads(data_hbogo)
                    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                         continue


                    for jj in data_resp_hbogo:
                        if jj["data_source"]=="HBOGO" and jj["type"]=="Program" and jj["sub_type"]=="MO":
                            hbogo_projectx_id.append(jj["projectx_id"])
                    if len(hbogo_projectx_id)>1:
                        s=s+1
                        writer.writerow({"hbogo_id OT":str(i[0]),"projectx_id_hbogo":hbogo_projectx_id,"Comment":'Multiple ingestion for same content of HBOGO'})

                    if len(hbogo_projectx_id)==1:
                        t=t+1

                    if len(hbogo_projectx_id)==0:
                        u=u+1
                        writer.writerow({"hbogo_id OT":str(i[0]),"projectx_id_hbogo":'',"Comment":'No Ingestion'})
                    print("total hbogo id OT :", total3 ,"No ingestion: ", u ,"Multiple mapped content :", s, "Total Fail: ", s+u, "Pass: ", t)

        print("total hbogo id MO:", total ,"No ingestion: ", l, "Multiple mapped content :", j, "Total Fail: ", l+j, "Pass: ", k)
        print("total hbogo id SE :", total1 ,"No ingestion: ", p, "Multiple mapped content :", q, "Total Fail: ", p+q, "Pass: ", r)
	print("total hbogo id SM :", total2 ,"No ingestion: ", m ,"Multiple mapped content :", n, "Total Fail: ", m+n, "Pass: ", o) 
	print("total hbogo id OT :", total3 ,"No ingestion: ", u ,"Multiple mapped content :", s, "Total Fail: ", s+u, "Pass: ", t)
 		
    print("total hbogo id :", total+total1+total2+total3 ,"total No ingestion: ", m+p+l+u, "Multiple mapped content :", s+q+n+j, "Total Fail: ", m+p+l+q+n+j+s+u, "Pass: ", r+o+k+t)
    print(datetime.datetime.now()) 

ingestion()


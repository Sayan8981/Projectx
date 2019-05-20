"""Writer: Saayan"""

import MySQLdb
import collections
from pprint import pprint
import os
import pymysql
import datetime
import httplib
import sys
import urllib2
import json
from urllib2 import HTTPError
import csv
import httplib
import socket
from urllib2 import URLError
import socket


def open_csv():
    inputFile="TVShows_mapped_ROVI_to_GB1"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_mapping_SM_new_API.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["Gb_sm_id","Rovi_sm_id","projectx_id_rovi","projectx_id_gb","Comment","Result of mapping series"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        j=0
        k=0
        l=0
        m=0
	n=0
        p=0
        total=0
        for r in range(1,len(fullist)):
	    gb_projectx_id=[]
	    rovi_projectx_id=[]
            total=total+1
            gb_sm_id=eval(fullist[r][1])
            rovi_sm_id=eval(fullist[r][0])
    
	    try:
                url_rovi="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi" %rovi_sm_id
                response_rovi=urllib2.Request(url_rovi)
                response_rovi.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_rovi=urllib2.urlopen(response_rovi)
                data_rovi=resp_rovi.read()
                data_resp_rovi=json.loads(data_rovi)

                url_gb="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=SM" %gb_sm_id
                response_gb=urllib2.Request(url_gb)
                response_gb.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_gb=urllib2.urlopen(response_gb)
                data_gb=resp_gb.read()
                data_resp_gb=json.loads(data_gb)
            except httplib.BadStatusLine:
                continue
            except urllib2.HTTPError:
                continue
            except socket.error:
                continue
            except urllib2.URLError:
                continue


	    for ii in data_resp_rovi:
                if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                    rovi_projectx_id.append(ii["projectx_id"])
            for jj in data_resp_gb:
                if jj["data_source"]=="GuideBox" and jj["type"]=="Program" and jj["sub_type"]=="SM":
                    gb_projectx_id.append(jj["projectx_id"])

            print gb_projectx_id
            print rovi_projectx_id
            
            if len(rovi_projectx_id)>1 or len(gb_projectx_id)>1:
                if gb_projectx_id and rovi_projectx_id:
                    j=j+1
                    a=gb_projectx_id
                    b=rovi_projectx_id
		    status='Nil'
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Result of mapping series":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi": rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Result of mapping series":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})

                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Result of mapping series":'Pass',"Comment":'Multiple ingestion for same content of GB'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi": rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Result of mapping series":'Fail',"Comment":'Fail:Multiple ingestion for same content of GB'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Multiple ingestion for same content of both sources',"Result of mapping series":'Pass'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Fail:Multiple ingestion for same content of both sources',"Result of mapping series":'Fail'})

            if len(rovi_projectx_id)==1 and len(gb_projectx_id)==1:
                if rovi_projectx_id == gb_projectx_id:
                    k=k+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":str(gb_projectx_id[0]),"Comment":'Pass',"Result of mapping series":'Pass'})

                if rovi_projectx_id != gb_projectx_id:
                    l=l+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":str(gb_projectx_id[0]),"Comment":'Fail',"Result of mapping series":'Fail'})

            if gb_projectx_id and not rovi_projectx_id:
                m=m+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":'Nil',"projectx_id_gb":gb_projectx_id,"Comment":'No ingestion of rovi_id',"Result of mapping series":'Fail'})

            if rovi_projectx_id and not gb_projectx_id:
                try:
                    duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=SM"%gb_sm_id
                    response_url=urllib2.Request(duplicate_api)
                    response_url.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_url=urllib2.urlopen(response_url)
                    data_url=resp_url.read()
                    data_resp_url=json.loads(data_url)
                    if data_resp_url==[]:
                        n=n+1
                        writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":'Nil',"Comment":'No ingestion of gb_id',"Result of mapping series":'Fail'})
                    else:
                        while len(data_resp_url)>1:
                            for i in data_resp_url:    
                                if data_resp_url.count(i)>1:
                                    data_resp_url.remove(i)
                            for i in data_resp_url:
                                if data_resp_url.count(i)>1:
                                    data_resp_url.remove(i)
                        if len(data_resp_url)==1:
                            for ll in data_resp_url:
                                gb_projectx_id.append(ll.get("projectx_id"))
                        if rovi_projectx_id==gb_projectx_id:
                            k=k+1
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(gb_projectx_id),"Comment":'Pass',"Result of mapping series":'Pass'})
                        else:
                            l=l+1
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(gb_projectx_id),"Comment":'Fail',"Result of mapping series":'Fail'})

                except httplib.BadStatusLine:
                    print ("exception caught httplib.BadStatusLine...........................................")
                    continue
                except urllib2.HTTPError:
                    print ("exception caught urllib2.HTTPError............................................")
                    continue
                except socket.error:
                    print ("exception caught socket.error............................................")
                    continue
                except URLError:
                    print ("exception caught URLerror............................................")
                    continue

            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":'',"projectx_id_gb":'',"Comment":'No Ingestion',"Result of mapping series":'N.A'})
	    print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "gb Id not ingested : ",n, "both not ingested: ",p)
    print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "gb Id not ingested : ",n, "both not ingested: ",p)
    print(datetime.datetime.now())


open_csv()


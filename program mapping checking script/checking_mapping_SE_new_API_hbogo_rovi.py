"""Writer: Saayan"""


import socket
import MySQLdb
import collections
from pprint import pprint
import sys
import urllib2
import json
import os
from urllib2 import HTTPError
import urllib
import csv
import os
import pymysql
import datetime
import httplib

def open_csv():
    inputFile="hbogo_rovi_mapping_episodes"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_mapping_episodes_API_hbogo_rovi.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["hbogo_id","Rovi_id","projectx_id_hbogo","projectx_id_rovi","Episode mapping","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        j=0
        k=0
        l=0
        m=0
        n=0
        p=0
        q=0
        z=0
        total=0
        for r in range(1,len(fullist)):
	    hbogo_projectx_id=[]
	    rovi_projectx_id=[]
            total=total+1
            hbogo_id=fullist[r][0]
            rovi_id=eval(fullist[r][1])
            print ("hbogo_id",hbogo_id)
            print ("rovi_id",rovi_id)
	    try:
	        url_rovi="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi" %rovi_id
                response_rovi=urllib2.Request(url_rovi)
                response_rovi.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_rovi=urllib2.urlopen(response_rovi)
                data_rovi=resp_rovi.read()
                data_resp_rovi=json.loads(data_rovi)

	        url_hbogo="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=HBOGO&showType=SE" %hbogo_id
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
	    
	    for ii in data_resp_rovi:
                if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                    rovi_projectx_id.append(ii["projectx_id"])
            for jj in data_resp_hbogo:
                if jj["data_source"]=="HBOGO" and jj["type"]=="Program" and jj["sub_type"]=="SE":
                    hbogo_projectx_id.append(jj["projectx_id"])	
            print hbogo_projectx_id
            print rovi_projectx_id
  
            if len(rovi_projectx_id)==1 and len(hbogo_projectx_id)==1:
                if rovi_projectx_id == hbogo_projectx_id:
                    j=j+1
                    writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_hbogo":str(hbogo_projectx_id[0]),"Episode mapping":'Pass',"Comment":'Pass'})
                if rovi_projectx_id != hbogo_projectx_id:
                    k=k+1
                    writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_hbogo":str(hbogo_projectx_id[0]),"Episode mapping":'Fail',"Comment":'Fail'})

            
	    if len(rovi_projectx_id)>1 or len(hbogo_projectx_id)>1:
                if hbogo_projectx_id and rovi_projectx_id:
                    l=l+1
                    a=hbogo_projectx_id
                    b=rovi_projectx_id
                    status='Nil'
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi": rovi_projectx_id,"projectx_id_hbogo":hbogo_projectx_id,"Episode mapping":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
                            writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_hbogo":hbogo_projectx_id,"Episode mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})
                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_hbogo":hbogo_projectx_id,"Episode mapping":'Pass',"Comment":'Multiple ingestion for same content of hbogo'})
                        if status=='Nil':
                            writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_hbogo":hbogo_projectx_id,"Episode mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of hbogo'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b: 
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_hbogo":hbogo_projectx_id,"Comment":'Multiple ingestion for same content of both sources',"Episode mapping":'Pass'})
                        if status=='Nil':
                            writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_hbogo": hbogo_projectx_id,"Comment":'Fail:Multiple ingestion for same content of both sources',"Episode mapping":'Fail'})

                if hbogo_projectx_id and not rovi_projectx_id:
                    m=m+1
                    writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":'Nil',"projectx_id_hbogo":hbogo_projectx_id,"Episode mapping":'N.A',"Comment":'No ingestion of rovi_id of episodes and multiple ingestion for hbogo id'})

                if rovi_projectx_id and not hbogo_projectx_id:
                    n=n+1
                    writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_hbogo":'Nil',"Episode mapping":'N.A',"Comment":'No ingestion of hbogo and multiple ingestion for Rovi_id'})

            if len(hbogo_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_hbogo":'',"Episode mapping":'N.A',"Comment":'No Ingestion of both sources'})

            if len(hbogo_projectx_id)==1 and len(rovi_projectx_id)==0:
                q=q+1
                writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_hbogo":str(hbogo_projectx_id[0]),"Episode mapping":'Fail',"Comment":'No Ingestion of rovi_id of episode'})
            if len(hbogo_projectx_id)==0 and len(rovi_projectx_id)==1:
                z=z+1
                writer.writerow({"hbogo_id":hbogo_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_hbogo":'',"Episode mapping":'Fail',"Comment":'No Ingestion of hbogo_id of episode'})
	    print("total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "hbogo Id not ingested : ",n+z, "both not ingested: ",p)

    print("total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "hbogo Id not ingested : ",n+z, "both not ingested: ",p)
    print(datetime.datetime.now())


open_csv()


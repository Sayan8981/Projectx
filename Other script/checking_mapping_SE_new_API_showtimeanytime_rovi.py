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
    inputFile="showtimeanytime_rovi_mapping_episodes"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_mapping_episodes_API_showtimeanytime_rovi.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["showtime_id","Rovi_id","projectx_id_showtime","projectx_id_rovi","Episode mapping","Comment"]
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
	    showtime_projectx_id=[]
	    rovi_projectx_id=[]
            total=total+1
            showtime_id=eval(fullist[r][0])
            rovi_id=eval(fullist[r][1])
            print ("showtime_id",showtime_id)
            print ("rovi_id",rovi_id)
            #query1="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='GuideBox' and type='Program' and sub_type='SE'"
            #query2="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='Rovi' and type='Program'"
	    try:
	        url_rovi="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi" %rovi_id
                response_rovi=urllib2.Request(url_rovi)
                response_rovi.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_rovi=urllib2.urlopen(response_rovi)
                data_rovi=resp_rovi.read()
                data_resp_rovi=json.loads(data_rovi)

	        url_showtime="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=ShowtimeAnyTime&showType=SE" %showtime_id
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
	    
	    for ii in data_resp_rovi:
                if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                    rovi_projectx_id.append(ii["projectx_id"])
            for jj in data_resp_showtime:
                if jj["data_source"]=="ShowtimeAnyTime" and jj["type"]=="Program" and jj["sub_type"]=="SE":
                    showtime_projectx_id.append(jj["projectx_id"])	
            print showtime_projectx_id
            print rovi_projectx_id
  
            if len(rovi_projectx_id)==1 and len(showtime_projectx_id)==1:
                if rovi_projectx_id == showtime_projectx_id:
                    j=j+1
                    writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_showtime":str(showtime_projectx_id[0]),"Episode mapping":'Pass',"Comment":'Pass'})
                if rovi_projectx_id != showtime_projectx_id:
                    k=k+1
                    writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_showtime":str(showtime_projectx_id[0]),"Episode mapping":'Fail',"Comment":'Fail'})
            
	    if len(rovi_projectx_id)>1 or len(showtime_projectx_id)>1:
                if showtime_projectx_id and rovi_projectx_id:
                    l=l+1
                    a=showtime_projectx_id
                    b=rovi_projectx_id
                    status='Nil'
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi": rovi_projectx_id,"projectx_id_showtime":showtime_projectx_id,"Episode mapping":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
                            writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_showtime":showtime_projectx_id,"Episode mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})
                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_showtime":showtime_projectx_id,"Episode mapping":'Pass',"Comment":'Multiple ingestion for same content of Showtime'})
                        if status=='Nil':
                            writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_showtime":showtime_projectx_id,"Episode mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of Showtime'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b: 
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_showtime":showtime_projectx_id,"Comment":'Multiple ingestion for same content of both sources',"Episode mapping":'Pass'})
                        if status=='Nil':
                            writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_showtime": showtime_projectx_id,"Comment":'Fail:Multiple ingestion for same content of both sources',"Episode mapping":'Fail'})

                if showtime_projectx_id and not rovi_projectx_id:
                    m=m+1
                    writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":'Nil',"projectx_id_showtime":showtime_projectx_id,"Episode mapping":'N.A',"Comment":'No ingestion of rovi_id of episodes and multiple ingestion for showtime_id'})

                if rovi_projectx_id and not showtime_projectx_id:
                    n=n+1
                    writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_showtime":'Nil',"Episode mapping":'N.A',"Comment":'No ingestion of showtime_id and multiple ingestion for Rovi_id'})

            if len(showtime_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_showtime":'',"Episode mapping":'N.A',"Comment":'No Ingestion of both sources'})

            if len(showtime_projectx_id)==1 and len(rovi_projectx_id)==0:
                q=q+1
                writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_showtime":str(showtime_projectx_id[0]),"Episode mapping":'Fail',"Comment":'No Ingestion of rovi_id of episode'})
            if len(showtime_projectx_id)==0 and len(rovi_projectx_id)==1:
                z=z+1
                writer.writerow({"showtime_id":showtime_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_showtime":'',"Episode mapping":'Fail',"Comment":'No Ingestion of showtime of episode'})
	    print("total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "showtime Id not ingested : ",n+z, "both not ingested: ",p)

    print("total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "showtime Id not ingested : ",n+r, "both not ingested: ",p)
    print(datetime.datetime.now())


open_csv()


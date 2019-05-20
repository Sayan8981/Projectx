"""Writer: Saayan"""

import MySQLdb
import collections
import sys
import urllib2
import json
import os
from urllib2 import HTTPError
import csv
import urllib
import os
import pymysql
import datetime
import httplib
import socket

def open_csv():
    inputFile="vudu_rovi_mapping_movies"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_mapping_movies_API_vudu_rovi.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["vudu_id","Rovi_id","projectx_id_rovi","projectx_id_vudu","Comment","Movies mapping"]
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
	    vudu_projectx_id=[]
	    rovi_projectx_id=[]
            total=total+1
            vudu_id=eval(fullist[r][0])
            rovi_id=str(fullist[r][1])
	    try:
	        url_rovi="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi"%eval(rovi_id)
	        response_rovi=urllib2.Request(url_rovi)
                response_rovi.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_rovi=urllib2.urlopen(response_rovi)
                data_rovi=resp_rovi.read()
                data_resp_rovi=json.loads(data_rovi)

	        url_vudu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Showtime&showType=MO" %hulu_id
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

      	    for ii in data_resp_rovi:
		if ii["data_source"]=='Rovi' and ii["type"]=='Program':
		    rovi_projectx_id.append(ii["projectx_id"])
	    for jj in data_resp_vudu:
		if jj["data_source"]=='vudu' and jj["type"]=='Program' and jj["sub_type"]=='MO':
                    vudu_projectx_id.append(jj["projectx_id"])

            print vudu_projectx_id
            print rovi_projectx_id
            
            if len(rovi_projectx_id)>1 or len(vudu_projectx_id)>1:
                if vudu_projectx_id and rovi_projectx_id:
                    j=j+1
                    a=vudu_projectx_id
                    b=rovi_projectx_id
		    status='Nil'
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
			    writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_vudu":str(vudu_projectx_id),"Movies mapping":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
			    writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_vudu":str(vudu_projectx_id),"Movies mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})

                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_vudu":vudu_projectx_id,"Movies mapping":'Pass',"Comment":'Multiple ingestion for same content of vudu'})
                        if status=='Nil':
                            writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_vudu":vudu_projectx_id,"Movies mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of vudu'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_vudu":vudu_projectx_id,"Comment":'Multiple ingestion for same content of both sources',"Movies mapping":'Pass'})
                        if status=='Nil':
                            writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_vudu":vudu_projectx_id,"Comment":'Fail:Multiple ingestion for same content of both sources',"Movies mapping":'Fail'})

            if len(rovi_projectx_id)==1 and len(vudu_projectx_id)==1:
                if rovi_projectx_id == vudu_projectx_id:
                    k=k+1
                    writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_vudu":str(vudu_projectx_id[0]),"Comment":'Pass',"Movies mapping":'Pass'})

                if rovi_projectx_id != vudu_projectx_id:
                    l=l+1
                    writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_vudu":str(vudu_projectx_id[0]),"Comment":'Fail',"Movies mapping":'Fail'})

            if vudu_projectx_id and not rovi_projectx_id:
                m=m+1
                writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":'Nil',"projectx_id_hulu":str(vudu_projectx_id),"Comment":'No ingestion of rovi_id',"Movies mapping":'Fail'})
            if rovi_projectx_id and not vudu_projectx_id:
                n=n+1
                writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_vudu":'Nil',"Comment":'No ingestion of vudu_id',"Movies mapping":'Fail'})
            if len(vudu_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"vudu_id":vudu_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_vudu":'',"Comment":'No Ingestion',"Movies mapping":'N.A'})
	    print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "vudu Id not ingested : ",n, "both not ingested: ",p)
    print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "vudu Id not ingested : ",n, "both not ingested: ",p)
    print(datetime.datetime.now())


open_csv()


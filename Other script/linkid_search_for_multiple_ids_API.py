"""Saayan"""



import pymongo
from pprint import pprint
import sys
import os
import csv
import pymysql
import collections
from pprint import pprint
import MySQLdb
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

class multiple_ids:

    def id_search(self):

        inputFile="all_gb_data_ott_new"
        f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
        reader = csv.reader(f)
        fullist=list(reader)

	result_sheet='/multiple_mapped_ids_for_same_source_id_API.csv'
        if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
        w=open(os.getcwd()+result_sheet,"wa")
        with w as mycsvfile:
            fieldnames = ["Service","projectx_ott_source_id","Link_Source_id","Projectx_ids which have same source_id","comment"]
            writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
            writer.writeheader()
	    total=0
	    Gb=0
	    Rv=0
	    GB_RV=0
            for rr in range(0,len(fullist)):
                total=total+1
                array=[]
                Service=str(fullist[rr][3])
                projectx_ott_source_id=str(fullist[rr][2]) 
                Link_Source_id=str(fullist[rr][1])
                try:
                    url_link="http://34.231.212.186:81/projectx/%s/%s/ottprojectx" %(Link_Source_id,Service)
                    response_link=urllib2.Request(url_link)
                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_link=urllib2.urlopen(response_link)
                    data_link=resp_link.read()
                    data_resp_link=json.loads(data_link)
                    for i in data_resp_link:
                        print i
                        if i.get("type")=='Program':
                            array.append({"projectx_id":i.get("projectx_id"),"data_source":i.get("data_source").encode(),"source_id":i.get("source_id").encode(),"type":i.get("type").encode(),"sub_type":i.get("sub_type").encode()})
                    if len(array)>1:
                        GB_RV=GB_RV+1
                        writer.writerow({"Service":Service,"projectx_ott_source_id":projectx_ott_source_id,"Link_Source_id":Link_Source_id,"Projectx_ids which have same source_id":array,"comment":'multiple mapped ids for same service_id '}) 
                    if len(array)==1:
                        if array[0].get("data_source")=='GuideBox':
                            Gb=Gb+1
                            print({"Service":Service,"projectx_ott_source_id":projectx_ott_source_id,"Link_Source_id":Link_Source_id,"Projectx_ids which have same source_id":array,"comment":'Mapped only with GuideBox'})
                        if array[0].get("data_source")=='Rovi':
                            Rv=Rv+1
                            print({"Service":Service,"projectx_ott_source_id":projectx_ott_source_id,"Link_Source_id":Link_Source_id,"Projectx_ids which have same source_id":array,"comment":'Mapped only with Rovi'})
                    print({"total":total,"multiple_ids":GB_RV,"only GB":Gb,"only Rovi":Rv})  

                except httplib.BadStatusLine:
                    print ("exception caught httplib.BadStatusLine...........................................")
                    continue
                except urllib2.HTTPError:
                    print ("exception caught HTTPError...........................................")
                    continue
                except socket.error:
                    print ("exception caught SocketError...........................................")
                    continue
                except URLError:
                    print ("exception caught URLError...........................................")
                    continue  
	print datetime.datetime.now()




a=multiple_ids()
a.id_search()

"""writer: Saayan"""

import urllib2
import json
import os
from pprint import pprint 
import urllib
import sys
import csv
import pymysql
import MySQLdb

#conn=pymysql.connect(user="caavo",passwd="Caavo@123",host="192.168.86.12",db="prjxmap")
#cur=conn.cursor()

class api:
        
    def data(self):
        k=0
        l=0
        arr=[]
        inputFile="No_ingestion_gb_ids"
    	f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    	reader = csv.reader(f)
        fullist=list(reader)

    	result_sheet='/Guidebox_ids_not present_API.csv'
    	if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
        w=open(os.getcwd()+result_sheet,"wa")
        
        with w as mycsvfile:
            fieldnames = ["Gb_id","Comment","type/source"]
            writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
            writer.writeheader()
            for j in range(1,len(fullist)):
                if str(fullist[j][2])=='No Ingestion':
                    k=k+1
              
                    gb_id=eval(fullist[j][0])
                    url="http://34.231.212.186:81/projectx/GuideBox/%d/source" %gb_id
                    print url
                    response=urllib2.Request(url)
                    response.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp=urllib2.urlopen(response)
                    data_=resp.read()
                    data_resp=json.loads(data_)
            
                    if not data_resp:
                        l=l+1
                        print({"Gb_id":gb_id,"Comment:":'Not present in API'})
                        writer.writerow({"Gb_id":gb_id,"Comment":'Not in API'})
                    else:
                        for i in data_resp:
                            if i["program_map_meta"]:
                                for i in i["program_map_meta"]:
                                    if i.get("data_source")=='GuideBox' and i.get("type")=='Program':         
                                        arr.append(gb_id)
                                        for j in arr:     
                                            print({"Gb_id":j, "Comment":"present in API","Sub_type":''}) 
                                                    
	    print("total:", k,"No ingestion:", l)	

a=api()
a.data()


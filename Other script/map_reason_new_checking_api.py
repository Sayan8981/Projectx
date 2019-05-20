import pymongo
from pprint import pprint
import sys
import os
import csv
import pymysql
import collections
from pprint import pprint
import MySQLdb
import re
import collections
from collections import Counter
import urllib2
import json
import urllib
from urllib2 import HTTPError
import datetime

def map_reason():
	
    conn1=pymysql.connect(user="projectx",passwd="projectx",host="18.232.135.33",db="projectx",port=3370)
    cur1=conn1.cursor()

    result_sheet='/validation_map_reason_new_checking_api.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with w as mycsvfile:
        fieldnames = ["projectx_id","Api response","api_Video_object","type","program_ott","service_in_api","missing_service","Result"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        arr=[]
	query="SELECT projectx_id FROM projectx.ProjectxMaps where data_source='GuideBox' and map_reason='new' and type='Program' and sub_type='SE';"
	cur1.execute(query,)
	res=cur1.fetchall()
	for i in res:
	    for x in i:
		arr.append(x)
	print len(arr)
	for x in arr:
	    try:
	        print x
		arr_source_api=[]
		arr_source_db=[]
	        query1="SELECT distinct(name) FROM projectx.ProgramOtts inner join projectx.OttSources on ProgramOtts.ott_source=OttSources.id where program_id=%s and ott_source in (47,147,150,118,56,50,5,192,188,337,340,142,59) and platform='pc';"
	        cur1.execute(query1,(x,))
                res1=cur1.fetchall()
	        url="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true" %x
                response=urllib2.Request(url)
                response.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp=urllib2.urlopen(response)
                data=resp.read()
                data_resp=json.loads(data)
	        if res1 and data_resp:
		    for i in res1:
                        arr_source_db.append(map(str,[k for k in list(i)]))
		    for j in data_resp:
	                if j["videos"]:
			    for i in j["videos"]:
				if i["fetched_from"]=="GuideBox":
			            arr_source_api.append([i.get("source_id").encode()])
		            e=Counter(map(str,[i for i in arr_source_db]))
			    f=Counter(map(str,[i for i in arr_source_api]))
			    comp=e-f
			    if list(comp.elements()):
                                writer.writerow({"projectx_id":str(x),"Api response":'True',"api_Video_object":'present',"type":'Program',"program_ott":'found in DB',"service_in_api":'',"missing_service":list(comp.elements()),"Result":'Fail'})
		        if not j["videos"]:
		            writer.writerow({"projectx_id":str(x),"Api response":'True',"api_Video_object":'not present',"type":'Program',"program_ott":'found in DB',"missing_service":arr_source_db,"Result":'Fail'})

	        if not res1 and data_resp:
                    for j in data_resp:
		        if j["videos"]:
			    for i in j["videos"]:
			        if i["fetched_from"]=="GuideBox":
				    arr_source_api.append([i.get("source_id").encode()])
                                    writer.writerow({"projectx_id":str(x),"Api response":'True',"api_Video_object":'present',"type":'Program',"program_ott":'not found in DB',"service_in_api":arr_source_api,"missing_service":'',"Result":''})

                        if not j["videos"]:
                            print({"projectx_id":str(x),"Api response":'True',"api_Video_object":'not present',"type":'Program',"program_ott":'not found in DB',"service_in_api":'',"missing_service":'',"Result":'no service available'})

	        if res1 and not data_resp:
		    for j in res1:
                        arr_source_db.append(map(str,[k for k in list(j)]))    
                    writer.writerow({"projectx_id":str(x),"Api response":'False',"api_Video_object":'not present',"type":'Program',"program_ott":'found in DB',"service_in_api":'',"missing_service":arr_source_db,"Result":'no service available'})
	        if not res1 and not data_resp:
                    print({"projectx_id":str(x),"Api response":'False',"api_Video_object":'not present',"type":'Program',"program_ott":'not found in DB',"service_in_api":'',"missing_service":'',"Result":'no service available'})
	    except HTTPError:
		print("exception caught")
		continue

    print datetime.datetime.now()

map_reason()

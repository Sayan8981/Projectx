
import urllib2
import json
import os
from pprint import pprint 
import urllib
import sys
import csv
import pymysql
import MySQLdb

print ("entering the id")
start_id=input(int)
end_id=input(int)
conn=pymysql.connect(user="caavo",passwd="Caavo@123",host="192.168.86.12",db="prjxmap")
cur=conn.cursor()

class api(object):
        
    def data(start_id,end_id):
        #import pdb;pdb.set_trace()
        for j in range(start_id,end_id):
	    print (j)
            url="http://34.231.212.186:81/projectx/%d/mapping" %j
            print url
            response=urllib2.Request(url)
            response.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
            resp=urllib2.urlopen(response)
            data=resp.read()
            data_resp=json.loads(data)
            rovi_id=[]
            gb_id=[]

            for i in data_resp:
                if i["data_source"]=='Rovi':
                    rovi_id.append(i["source_id"].encode())
            for i in data_resp:
                if i["data_source"]=='GuideBox':
                    gb_id.append(i["source_id"].encode())      
            print('rovi_id:',rovi_id)
            print('gb_id:',gb_id)

            if len(rovi_id)>1 and len(gb_id)>1:
                for i in rovi_id:
                    query="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'Rovi'),)
                for i in gb_id:
                    query1="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'GB'),)
            if len(rovi_id)>1 and len(gb_id)==1:
                for i in rovi_id:
                    query="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'Rovi'),)
                for i in gb_id:
                    query1="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'GB'),)
            if len(gb_id)>1 and len(rovi_id)==1:
                for i in rovi_id:
                    query="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'Rovi'),)
                for i in gb_id:
                    query1="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'GB'),)
            if len(rovi_id)==1 and len(gb_id)==1:
                query="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                cur.execute(query,(j,','.join(map(str,[i[:] for i in rovi_id])),'Rovi'))
                cur.execute(query,(j,','.join(map(str,[i[:] for i in gb_id])),'GB'))
            if len(rovi_id)>1 and gb_id==[]:
                for i in rovi_id:
                    query="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'Rovi'),)
                
            if len(gb_id)>1 and rovi_id==[]:
		for i in gb_id:
                    query1="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'GB'),)
            if len(rovi_id)==1 and gb_id==[]:
                for i in rovi_id:
                    query="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'Rovi'),)

            if len(gb_id)==1 and rovi_id==[]:
		for i in gb_id:
                    query1="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
                    cur.execute(query,(j,i,'GB'),)

            if rovi_id==[] and gb_id==[]:
               query="insert into map (prjx_id,source_id,type) values(%s,%s,%s)"
               cur.execute(query,(j,'','None'))
    conn.autocommit(True)
    data(start_id,end_id)    
          

  

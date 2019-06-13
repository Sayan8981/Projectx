"""Saayan"""

import threading
import pymongo
from pprint import pprint
import sys
import os
import csv
import re
import pymysql
import collections
from pprint import pprint
import MySQLdb
import re
import collections
from collections import Counter
from urllib2 import URLError
import socket
import datetime
import _mysql_exceptions
import unidecode
import urllib2
import json
import urllib
import httplib
from urllib2 import HTTPError
import unidecode
sys.setrecursionlimit(1500)

def ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer):
    #import pdb;pdb.set_trace()
    try:
        Token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        domain_name="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com"
        token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        print ("\n")
        print (gb_id,gb_show_type,name)
        if gb_show_type=='MO':
            guidebox_mapping_api='http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=MO'%gb_id
            GB_link=urllib2.Request(guidebox_mapping_api)
            GB_link.add_header('Authorization',token)
            GB_resp=urllib2.urlopen(GB_link)
            data_GB=GB_resp.read()
            data_GB_resp_mo=json.loads(data_GB)
            if data_GB_resp_mo!=[]:
                #["gb_sm_id","gb_id","gb_show_type","Duplicate_present","Series_duplicate_present","Ingested","Not_ingested","Data_present_in_dump"]
                writer.writerow(['',gb_id,gb_show_type,'','','True','',''])
            else:
                gb_duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=MO'%gb_id
                GB_link=urllib2.Request(gb_duplicate_api)
                GB_link.add_header('Authorization',token)
                GB_resp=urllib2.urlopen(GB_link)
                data_GB=GB_resp.read()
                data_GB_resp_duplicate=json.loads(data_GB)
                if data_GB_resp_duplicate!=[]:
                    writer.writerow(['',gb_id,gb_show_type,'True','','True','',''])
                else:
                    gb_present_data='http://34.231.212.186:81/projectx/guidebox?sourceId=%d&showType=MO'%gb_id
                    GB_link=urllib2.Request(gb_present_data)
                    GB_link.add_header('Authorization',token)
                    GB_resp=urllib2.urlopen(GB_link)
                    data_GB=GB_resp.read()
                    data_GB_resp=json.loads(data_GB)
                    if data_GB_resp==True:
                        writer.writerow(['',gb_id,gb_show_type,'False','','','True','True'])           
                    else:
                        writer.writerow(['',gb_id,gb_show_type,'False','','','True','False'])    
        if gb_show_type=='SE':
            guidebox_mapping_api='http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=SE'%gb_id
            GB_link=urllib2.Request(guidebox_mapping_api)
            GB_link.add_header('Authorization',token)
            GB_resp=urllib2.urlopen(GB_link)
            data_GB=GB_resp.read()
            data_GB_resp_se=json.loads(data_GB)
            if data_GB_resp_se!=[]:
                #["gb_sm_id","gb_id","gb_show_type","Duplicate_present","Series_duplicate_present","Ingested","Not_ingested","Data_present_in_dump"]
                writer.writerow([gb_sm_id,gb_id,gb_show_type,'','','True','',''])
            else:
                gb_duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=SM'%gb_sm_id
                GB_link=urllib2.Request(gb_duplicate_api)
                GB_link.add_header('Authorization',token)
                GB_resp=urllib2.urlopen(GB_link)
                data_GB=GB_resp.read()
                data_GB_resp_duplicate=json.loads(data_GB)
                if data_GB_resp_duplicate!=[]:
                    writer.writerow([gb_sm_id,gb_id,gb_show_type,'','True','True','',''])
                else:
                    gb_present_data='http://34.231.212.186:81/projectx/guidebox?sourceId=%d&showType=SE'%gb_id
                    GB_link=urllib2.Request(gb_present_data)
                    GB_link.add_header('Authorization',token)
                    GB_resp=urllib2.urlopen(GB_link)
                    data_GB=GB_resp.read()
                    data_GB_resp=json.loads(data_GB)
                    if data_GB_resp==True:
                        writer.writerow([gb_sm_id,gb_id,gb_show_type,'False','False','','True','True'])           
                    else:
                        writer.writerow([gb_sm_id,gb_id,gb_show_type,'False','False','','True','False'])
        if gb_show_type=='SM':
            guidebox_mapping_api='http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=SM'%gb_id
            GB_link=urllib2.Request(guidebox_mapping_api)
            GB_link.add_header('Authorization',token)
            GB_resp=urllib2.urlopen(GB_link)
            data_GB=GB_resp.read()
            data_GB_resp_sm=json.loads(data_GB)
            if data_GB_resp_sm!=[]:
                writer.writerow(['',gb_id,gb_show_type,'','','True','',''])
            else:
                gb_duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=SM'%gb_id
                GB_link=urllib2.Request(gb_duplicate_api)
                GB_link.add_header('Authorization',token)
                GB_resp=urllib2.urlopen(GB_link)
                data_GB=GB_resp.read()
                data_GB_resp_duplicate=json.loads(data_GB)
                if data_GB_resp_duplicate!=[]:
                    #["gb_sm_id","gb_id","gb_show_type","Duplicate_present","Series_duplicate_present","Ingested","Not_ingested","Data_present_in_dump"]
                    writer.writerow(['',gb_id,gb_show_type,'True','','True','',''])
                else:
                    gb_present_data='http://34.231.212.186:81/projectx/guidebox?sourceId=%d&showType=SM'%gb_id
                    GB_link=urllib2.Request(gb_present_data)
                    GB_link.add_header('Authorization',token)
                    GB_resp=urllib2.urlopen(GB_link)
                    data_GB=GB_resp.read()
                    data_GB_resp=json.loads(data_GB)
                    if data_GB_resp==True:
                        writer.writerow(['',gb_id,gb_show_type,'False','','','True','True'])           
                    else:
                        writer.writerow(['',gb_id,gb_show_type,'False','','','True','False'])

    except httplib.BadStatusLine:
        print ("exception caught httplib.BadStatusLine................................................................................",name,gb_id,gb_sm_id,gb_show_type)
        print ("\n") 
        print ("Retrying.............")
        print ("\n")    
        ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)
    except urllib2.HTTPError:
        print ("exception caught HTTPError................................................................................",name,gb_id,gb_sm_id,gb_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)
    except socket.error:
        print ("exception caught SocketError..........................................................................................",name,gb_id,gb_sm_id,gb_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)   
    except URLError:
        print ("exception caught URLError.............................................................................................",name,gb_id,gb_sm_id,gb_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)
    except RuntimeError:
        print ("exception caught runtimeerror..................................................................................",name,gb_id,gb_sm_id,gb_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)                         
    except ValueError:
        print ("exception caught valueerror ..................................................................................",name,gb_id,gb_sm_id,gb_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)
    except Exception as e:
        print ("exception caught ..................................................................................",name,type(e),gb_id,gb_sm_id,gb_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)

def getting_ids(start,name,end,id):
    print name
    print("Checking ingestion of series and Movies to Projectx search api............................................")

    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["ozone"]
    mytable=mydb["guidebox_program_details"]

    result_sheet='/Ingestion_checked_in_Px%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with w as mycsvfile:
        fieldnames = ["gb_sm_id","gb_id","gb_show_type","Duplicate_present","Series_duplicate_present","Ingested","Not_ingested","Data_present_in_dump"]

        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        gb_mo_list=[]
        gb_sm_list=[]
        gb_se_list=[]
        total=[]
        for aa in range(start,end,1000):
            #import pdb;pdb.set_trace()
            print ("\n")
            print ("mytable.aggregate([{'$skip':%d},{'$limit':1000}")%aa
            GB_query=mytable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"$and":[{"show_type":{"$in":["SE","MO","SM"]}}]}},{"$project":{"gb_id":1,"show_id":1,"_id":0,"show_type":1}}]) 
            print ("\n")
            print({"start": start,"end":end})
            print ("\n")
            for i in GB_query:
                gb_id=i.get("gb_id")
                gb_sm_id=i.get("show_id")
                gb_show_type=i.get("show_type").encode('utf-8')
                if gb_id is not None:
                    if gb_show_type=='MO':
                        if gb_id not in gb_mo_list:
                            gb_mo_list.append(gb_id)
                            ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)
                    if gb_show_type=='SM':
                        if gb_id not in gb_sm_list:
                            gb_sm_list.append(gb_id)
                            ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)
                    if gb_show_type=='SE':
                        if gb_id not in gb_se_list:
                            gb_se_list.append(gb_id)
                            ingestion_checking(gb_id,gb_sm_id,gb_show_type,name,writer)                

                print("\n")                             
                """print ({"Total MO":len(gb_mo_list),"Thread_name":name})
                print("\n")                             
                print ({"Total SM":len(gb_sm_list),"Thread_name":name})
                print("\n")                             
                print ({"Total SE":len(gb_se_list),"Thread_name":name})
                print("\n")"""                             
                print ({"Total":len(gb_mo_list)+len(gb_se_list)+len(gb_sm_list),"Thread_name":name})
                print ("\n")
                print datetime.datetime.now()
    connection.close()
    
t1=threading.Thread(target=getting_ids,args=(0,"thread - 1",50000,1))
t1.start()
t2=threading.Thread(target=getting_ids,args=(50000,"thread - 2",100000,2))
t2.start()                          
t3=threading.Thread(target=getting_ids,args=(100000,"thread - 3",150000,3))
t3.start()
t4=threading.Thread(target=getting_ids,args=(150000,"thread - 4",200000,4))
t4.start()
t5=threading.Thread(target=getting_ids,args=(200000,"thread - 5",250000,5))
t5.start()
t6=threading.Thread(target=getting_ids,args=(250000,"thread - 6",300000,6))
t6.start()
t7=threading.Thread(target=getting_ids,args=(300000,"thread - 7",350000,7))
t7.start()
t8=threading.Thread(target=getting_ids,args=(350000,"thread - 8",400000,8))
t8.start()
t9=threading.Thread(target=getting_ids,args=(400000,"thread - 9",450000,9))
t9.start()
t10=threading.Thread(target=getting_ids,args=(450000,"thread - 10",500000,10))
t10.start()                          
t11=threading.Thread(target=getting_ids,args=(500000,"thread - 11",550000,11))
t11.start()
t12=threading.Thread(target=getting_ids,args=(550000,"thread - 12",600000,12))
t12.start()
t13=threading.Thread(target=getting_ids,args=(600000,"thread - 13",650000,13))
t13.start()
t14=threading.Thread(target=getting_ids,args=(650000,"thread - 14",700000,14))
t14.start()
t15=threading.Thread(target=getting_ids,args=(700000,"thread - 15",750000,15))
t15.start()
t16=threading.Thread(target=getting_ids,args=(750000,"thread - 16",800000,16))
t16.start()
t17=threading.Thread(target=getting_ids,args=(800000,"thread - 17",850000,17))
t17.start()
t18=threading.Thread(target=getting_ids,args=(850000,"thread - 18",900000,18))
t18.start()
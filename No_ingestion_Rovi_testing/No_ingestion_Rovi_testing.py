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

def ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer):
    #import pdb;pdb.set_trace()
    try:
        Token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        domain_name="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com"
        token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        print ("\n")
        print (rovi_id,rovi_show_type,name)
        if rovi_show_type=='MO' or rovi_show_type=='SE' or rovi_show_type=='SM':
            rovi_mapping_api='http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi'%eval(rovi_id)
            rovi_link=urllib2.Request(rovi_mapping_api)
            rovi_link.add_header('Authorization',token)
            rovi_resp=urllib2.urlopen(rovi_link)
            data_rovi=rovi_resp.read()
            data_rovi_resp=json.loads(data_rovi)
            if data_rovi_resp!=[]:
                #["rovi_sm_id","rovi_id","rovi_show_type","Duplicate_present","Series_duplicate_present","Ingested","Not_ingested","Data_present_in_dump"]
                writer.writerow([rovi_sm_id,rovi_id,rovi_show_type,'','','True','','']) 
            else:
                writer.writerow([rovi_sm_id,rovi_id,rovi_show_type,'','','False','',''])       

    except httplib.BadStatusLine:
        print ("exception caught httplib.BadStatusLine................................................................................",name,rovi_id,rovi_sm_id,rovi_show_type)
        print ("\n") 
        print ("Retrying.............")
        print ("\n")    
        ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)
    except urllib2.HTTPError:
        print ("exception caught HTTPError................................................................................",name,rovi_id,rovi_sm_id,rovi_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)
    except socket.error:
        print ("exception caught SocketError..........................................................................................",name,rovi_id,rovi_sm_id,rovi_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)   
    except URLError:
        print ("exception caught URLError.............................................................................................",name,rovi_id,rovi_sm_id,rovi_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)
    except RuntimeError:
        print ("exception caught runtimeerror..................................................................................",name,rovi_id,rovi_sm_id,rovi_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)                         
    except ValueError:
        print ("exception caught valueerror ..................................................................................",name,rovi_id,rovi_sm_id,rovi_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)
    except Exception as e:
        print ("exception caught ..................................................................................",name,type(e),rovi_id,rovi_sm_id,rovi_show_type)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)

def getting_ids(start,name,end,id):
    print name
    print("Checking ingestion of series and Movies to Projectx search api............................................")

    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["qadb"]
    mytable=mydb["program_general"]

    result_sheet='/Ingestion_checked_in_Px%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with w as mycsvfile:
        fieldnames = ["rovi_sm_id","rovi_id","rovi_show_type","Duplicate_present","Series_duplicate_present","Ingested","Not_ingested","Data_present_in_dump"]

        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        rovi_mo_list=[]
        rovi_sm_list=[]
        rovi_se_list=[]
        total=[]
        for aa in range(start,end,1000):
            #import pdb;pdb.set_trace()
            print ("\n")
            print ("mytable.aggregate([{'$skip':%d},{'$limit':1000}")%aa
            rovi_query=mytable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"$and":[{"show type":{"$in":["SE","MO","SM"]}}]}},{"$project":{"program id":1,"series id":1,"_id":0,"show type":1}}]) 
            print ("\n")
            print({"start": start,"end":end})
            print ("\n")
            for i in rovi_query:
                rovi_id=i.get("program id")
                rovi_sm_id=i.get("series id")
                rovi_show_type=i.get("show type").encode('utf-8')
                if rovi_id is not None or rovi_id!="":
                    if rovi_show_type=='MO':
                        if rovi_id not in rovi_mo_list:
                            rovi_mo_list.append(rovi_id)
                            ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)
                    if rovi_show_type=='SM':
                        if rovi_id not in rovi_sm_list:
                            rovi_sm_list.append(rovi_id)
                            ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)
                    if rovi_show_type=='SE':
                        if rovi_id not in rovi_se_list:
                            rovi_se_list.append(rovi_id)
                            ingestion_checking(rovi_id,rovi_sm_id,rovi_show_type,name,writer)                

                print("\n")                             
                """print ({"Total MO":len(rovi_mo_list),"Thread_name":name})
                print("\n")                             
                print ({"Total SM":len(rovi_sm_list),"Thread_name":name})
                print("\n")                             
                print ({"Total SE":len(rovi_se_list),"Thread_name":name})
                print("\n")"""                             
                print ({"Total":len(rovi_mo_list)+len(rovi_se_list)+len(rovi_sm_list),"Thread_name":name})
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
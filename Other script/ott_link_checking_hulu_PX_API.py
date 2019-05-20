"""writer: Saayan"""

import threading
import httplib
import socket
import urllib2
import MySQLdb
import collections
from pprint import pprint
import sys
import csv
import os
import pymysql
import datetime
from urllib2 import HTTPError
from collections import Counter
import json
import pymongo

def ott_link_checking_hulu_se(start,name,end,id):

    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["qadb"]
    mytable=mydb["HuluValidEpisodes"]

    result_sheet='/hulu_ott_link_checking_PX_SE_API%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["hulu_id","title","programming_type","show_type","series_title","duration","description","hulu_link","projectx_id_hulu","Link_present in PX API","Missing ott_contents","Additional/Duplicates ott_contents","Result","Comment","Hulu Link expired","Multiple Mapped ids"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        TOTAL=0
        total=0
        for aa in range(start,end,10):
	    arr_hulu=[]
            query1=mytable.aggregate([{"$skip":aa},{"$limit":10},{"$match":{"programming_type":"Full Episode"}},{"$project":{"_id":0,"id":1,"series":1,"duration":1,"title":1,"site_id":1,"type":1,"description":1,"link":1,"programming_type":1}}])
            for ii in query1:
                arr_hulu.append(ii)
            print len(arr_hulu)
  	    KEY1="hulu"
            for i in arr_hulu: 
	        TOTAL=TOTAL+1
	        arr_link_hulu=[]
                dict1_=dict()
                dict1_.setdefault(KEY1,[])
	        arr_link_hulu.append(i.get("site_id"))
	        dict1_[KEY1].append(arr_link_hulu[0])
	        print ("hulu_id :", i.get("id"))
                if i.get("programming_type").encode()=='Full Episode':
                    try:
		        hulu_projectx_id=[]
                        url_hulu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Showtime&showType=SE" %i.get("id")
                        response_hulu=urllib2.Request(url_hulu)
                        response_hulu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_hulu=urllib2.urlopen(response_hulu)
                        data_hulu=resp_hulu.read()
                        data_resp_hulu=json.loads(data_hulu)
                
                        for ii in data_resp_hulu:
		            if ii["data_source"]=='hulu' and ii["type"]=='Program' and ii["sub_type"]=='SE':
                                hulu_projectx_id.append(ii["projectx_id"])
		        print("hulu_projectx_id", hulu_projectx_id)
                        if len(hulu_projectx_id)>1:
			    ids_present=['multiple mapped ids present']
		        else:
		   	    ids_present=['']
                        for y in hulu_projectx_id:
		   	    total=total+1
                            arr_link_px=[]
			    dict2_=dict()
                            url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%y
			    response_px=urllib2.Request(url_px)
                            response_px.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_px=urllib2.urlopen(response_px)
                            data_px=resp_px.read()
                            data_resp_px=json.loads(data_px)

                            for kk in data_resp_px:
                                for ll in kk["videos"]:
                                    if ll["source_id"]=="hulu" and ll["platform"]=='pc':
		  		        arr_link_px.append(ll["source_program_id"].encode())
                            if len(arr_link_px)>1:
                                for mm in arr_link_px:
				    if arr_link_px.count(mm)>1:
				        arr_link_px.remove(mm)

		   	    comp1=''

			    if arr_link_px!=[]:
                                if str(i.get("id").encode()) in arr_link_px:
                                    dict2_.setdefault(KEY1, [])
                                    dict2_[KEY1].append(arr_link_px)
				    comp1=''
                                else:
                                    dict2_.setdefault(KEY1, [])
                                    dict2_[KEY1].append(arr_link_px)
				    comp1='additional'
		  	    else:
			        dict2_.setdefault(KEY1, [])
                                dict2_[KEY1].append(arr_link_hulu[0])
			        comp1='missing'
	
		            additional=[]
		            missing=[]
                            if comp1=='':
                                additional=["Pass",'[]','No additional/duplicates ott link present']
                                missing=["Pass",'','No missing ott link','']
                            if comp1=='additional':
                                expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%d&service_short_name=hulu"%i.get("id")
                                response_expired=urllib2.Request(expired_url)
                                response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                                resp_expired=urllib2.urlopen(response_expired)
                                data_expired=resp_expired.read()
                                data_resp_expired=json.loads(data_expired)
                                if data_resp_expired['is_available']==False:
                                    additional=["Fail",dict2_,'additional/duplicates ott link present']
                                    missing=["Fail",dict1_,'missing ott link present','No']
                                if data_resp_expired['is_available']==True:
                                    additional=["Fail",dict2_,'additional/duplicates ott link present']
                                    missing=["Pass",'','missing ott link present','expired'] 
  			    if comp1=='missing' :
                                expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%d&service_short_name=hulu"%i.get("id")
                                response_expired=urllib2.Request(expired_url)
                                response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                                resp_expired=urllib2.urlopen(response_expired)
                                data_expired=resp_expired.read()
                                data_resp_expired=json.loads(data_expired)
                                if data_resp_expired['is_available']==False:
                                    additional=["Pass",'','No additional/duplicates ott link present']
                                    missing=["Fail",dict2_,'missing ott link present','No']
                                if data_resp_expired['is_available']==True:
                                    additional=["Pass",'','No additional/duplicates ott link present']
                                    missing=["Pass",'','missing ott link present','expired']

		            if missing[0]=='Pass' and additional[0]=='Pass':
               	  	        writer.writerow({"hulu_id":i.get("id"),"title":i.get("title").encode(),"programming_type":i.get("programming_type").encode(),"show_type":i.get("type").encode(),"series_title":i.get("series").get("name").encode(),"duration":i.get("duration"),"description":i.get("description"),"hulu_link":i.get("link").encode(),"projectx_id_hulu":y,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":'',"Result":'Pass',"hulu Link expired":missing[3],"Comment":'Pass',"Multiple Mapped ids":ids_present[0]})

            	            if missing[0]=='Pass' and additional[0]=='Fail':
			        writer.writerow({"hulu_id":i.get("id"),"title":i.get("title").encode(),"programming_type":i.get("programming_type").encode(),"show_type":i.get("type").encode(),"series_title":i.get("series").get("name").encode(),"duration":i.get("duration"),"description":i.get("description"),"hulu_link":i.get("link").encode(),"projectx_id_hulu":y,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hulu Link expired":missing[3],"Comment":'Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})	

            	            if missing[0]=='Fail' and additional[0]=='Fail':
			        writer.writerow({"hulu_id":i.get("id"),"title":i.get("title").encode(),"programming_type":i.get("programming_type").encode(),"show_type":i.get("type").encode(),"series_title":i.get("series").get("name").encode(),"duration":i.get("duration"),"description":i.get("description"),"hulu_link":i.get("link").encode(),"projectx_id_hulu":y,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hulu Link expired":missing[3],"Comment":'Missing and Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})

                            if missing[0]=='Fail' and additional[0]=='Pass':
                                writer.writerow({"hulu_id":i.get("id"),"title":i.get("title").encode(),"programming_type":i.get("programming_type").encode(),"show_type":i.get("type").encode(),"series_title":i.get("series").get("name").encode(),"duration":i.get("duration"),"description":i.get("description"),"hulu_link":i.get("link").encode(),"projectx_id_hulu":y,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":'',"Result":'Fail',"hulu Link expired":missing[3],"Comment":'Missing and Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})
		        hulu_projectx_id=[]
		    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                        continue
                print ({"TOTAL SE": TOTAL,"total ingested":total})
            print ({"TOTAL SE": TOTAL,"total ingested":total})
    print datetime.datetime.now()
    connection.close()

def ott_link_checking_hulu_mo(start,name,end,id):
            
    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["qadb"]
    mytable=mydb["HuluValidMovies"]

    result_sheet='/hulu_ott_link_checking_PX_MO_API%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["hulu_id","title","programming_type","show_type","series_title","duration","description","hulu_link","projectx_id_showtime","Link_present in PX API","Missing ott_contents","Additional/Duplicates ott_contents","Result","Comment","Hulu Link expired","Multiple Mapped ids"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        TOTAL=0
        total=0
        for aa in range(start,end,10):
            arr_hulu=[]
            query1=mytable.aggregate([{"$skip":aa},{"$limit":10},{"$match":{"programming_type":"Full Movie"}},{"$project":{"_id":0,"id":1,"duration":1,"title":1,"site_id":1,"type":1,"description":1,"link":1,"programming_type":1}}])
            for ii in query1:
                arr_hulu.append(ii)
            print len(arr_hulu)
            KEY1="hulu"
            for i in arr_hulu:
                TOTAL=TOTAL+1
                arr_link_hulu=[]
                dict1_=dict()
                dict1_.setdefault(KEY1,[])
                arr_link_hulu.append(i.get("site_id"))
                dict1_[KEY1].append(arr_link_hulu[0])
                print ("hulu_id :", i.get("id"))

                if i.get("programming_type").encode()=='Full Movie':
                    try:
                        hulu_projectx_id=[]
                        url_hulu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Showtime&showType=SE" %i.get("id")
                        response_hulu=urllib2.Request(url_hulu)
                        response_hulu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_hulu=urllib2.urlopen(response_hulu)
                        data_hulu=resp_hulu.read()
                        data_resp_hulu=json.loads(data_hulu)

                        for ii in data_resp_hulu:
                            if ii["data_source"]=='hulu' and ii["type"]=='Program' and ii["sub_type"]=='MO':
                                hulu_projectx_id.append(ii["projectx_id"])
                        print("hulu_projectx_id", hulu_projectx_id)
                        if len(hulu_projectx_id)>1:
                            ids_present=['multiple mapped ids present']
                        else:
                            ids_present=['']
                        for y in hulu_projectx_id:
                            total=total+1
                            arr_link_px=[]
                           
                            dict2_=dict()
                            url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%y
                            response_px=urllib2.Request(url_px)
                            response_px.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_px=urllib2.urlopen(response_px)
                            data_px=resp_px.read()
                            data_resp_px=json.loads(data_px)

                            for kk in data_resp_px:
                                for ll in kk["videos"]:
                                    if ll["source_id"]=="hulu" and ll["platform"]=='pc':
                                        arr_link_px.append(ll["source_program_id"].encode())
                            if len(arr_link_px)>1:
                                for mm in arr_link_px:
                                    if arr_link_px.count(mm)>1:
                                        arr_link_px.remove(mm)

                            comp1=''
                            if arr_link_px!=[]:
                                if str(i.get("id").encode()) in arr_link_px:
                                    dict2_.setdefault(KEY1, [])
                                    dict2_[KEY1].append(arr_link_px)
                                    comp1=''
                                else:
                                    dict2_.setdefault(KEY1, [])
                                    dict2_[KEY1].append(arr_link_px)
                                    comp1='additional'
                            else:
                                dict2_.setdefault(KEY1, [])
                                dict2_[KEY1].append(arr_link_hulu[0])
                                comp1='missing'

                            additional=[]
                            missing=[]
                            if comp1=='':
                                additional=["Pass",'[]','No additional/duplicates ott link present']
                                missing=["Pass",'','No missing ott link','']
                            if comp1=='additional':
                                expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%d&service_short_name=hulu"%i.get("id")
                                response_expired=urllib2.Request(expired_url)
                                response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                                resp_expired=urllib2.urlopen(response_expired)
                                data_expired=resp_expired.read()
                                data_resp_expired=json.loads(data_expired)
                                if data_resp_expired['is_available']==False:
                                    additional=["Fail",dict2_,'additional/duplicates ott link present']
                                    missing=["Fail",dict1_,'missing ott link present','No']
                                if data_resp_expired['is_available']==True:
                                    additional=["Fail",dict2_,'additional/duplicates ott link present']
                                    missing=["Pass",'','missing ott link present','expired']
                            if comp1=='missing' :
                                expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%d&service_short_name=hulu"%i.get("id")
                                response_expired=urllib2.Request(expired_url)
                                response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                                resp_expired=urllib2.urlopen(response_expired)
                                data_expired=resp_expired.read()
                                data_resp_expired=json.loads(data_expired)
                                if data_resp_expired['is_available']==False:
                                    additional=["Pass",'','No additional/duplicates ott link present']
                                    missing=["Fail",dict2_,'missing ott link present','No']
                                if data_resp_expired['is_available']==True:
                                    additional=["Pass",'','No additional/duplicates ott link present']
                                    missing=["Pass",'','missing ott link present','expired']

                            if missing[0]=='Pass' and additional[0]=='Pass':
                                writer.writerow({"hulu_id":i.get("id"),"title":i.get("title").encode(),"programming_type":i.get("programming_type").encode(),"show_type":i.get("type").encode(),"series_title":'',"duration":i.get("duration"),"description":i.get("description"),"hulu_link":i.get("link").encode(),"projectx_id_hulu":y,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":'',"Result":'Pass',"hulu Link expired":missing[3],"Comment":'Pass',"Multiple Mapped ids":ids_present[0]})

                            if missing[0]=='Pass' and additional[0]=='Fail':
                                writer.writerow({"hulu_id":i.get("id"),"title":i.get("title").encode(),"programming_type":i.get("programming_type").encode(),"show_type":i.get("type").encode(),"duration":i.get("duration"),"description":i.get("description"),"hulu_link":i.get("link").encode(),"projectx_id_hulu":y,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hulu Link expired":missing[3],"Comment":'Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})

                            if missing[0]=='Fail' and additional[0]=='Fail':
                                writer.writerow({"hulu_id":i.get("id"),"title":i.get("title").encode(),"programming_type":i.get("programming_type").encode(),"show_type":i.get("type").encode(),"series_title":'',"duration":i.get("duration"),"description":i.get("description"),"hulu_link":i.get("link").encode(),"projectx_id_hulu":y,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hulu Link expired":missing[3],"Comment":'Missing and Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})

                            if missing[0]=='Fail' and additional[0]=='Pass':
                                writer.writerow({"hulu_id":i.get("id"),"title":i.get("title").encode(),"programming_type":i.get("programming_type").encode(),"show_type":i.get("type").encode(),"series_title":'',"duration":i.get("duration"),"description":i.get("description"),"hulu_link":i.get("link").encode(),"projectx_id_hulu":y,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":'',"Result":'Fail',"hulu Link expired":missing[3],"Comment":'Missing and Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})
                        hulu_projectx_id=[]
                    except httplib.BadStatusLine:
                        continue
                    except urllib2.HTTPError:
                        continue
                    except socket.error:
                        continue
                print ({"TOTAL MO": TOTAL,"total ingested":total})
            print ({"TOTAL MO": TOTAL,"total ingested":total})

    print datetime.datetime.now()
    connection.close()



t1=threading.Thread(target=ott_link_checking_hulu_se,args=(0,"thread-1",20,1))
t1.start()
t2=threading.Thread(target=ott_link_checking_hulu_mo,args=(0,"thread-2",20,2))
t2.start()

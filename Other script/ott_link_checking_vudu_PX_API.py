"""writer: Saayan"""
                                                          #189024=MO+SE, SM=6110
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

def ott_link_checking_vudu(start,name,end,id):

    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["qadb"]
    mytable=mydb["vududump"]

    result_sheet='/vudu_ott_link_checking_PX_API%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["vudu_id","title","show_type","series_title","release_year","episode_number","season_number","vudu_link","projectx_id_vudu","Link_present in PX API","Missing ott_contents","Additional/Duplicates ott_contents","Result","Comment","vudu Link expired","Multiple Mapped ids"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        TOTAL=0
        total=0
        for aa in range(start,end,10):
            query1=mytable.aggregate([{"$skip":aa},{"$limit":10},{"$match":{"show_type":{"$in":["MO","SE"]}}},{"$project":{"launch_id":1,"_id":0,"show_type":1,"title":1,"release_year":1,"series_title":1,"episode_number":1,"season_number":1,"purchase_types":1,"url":1}}])
            for ii in query1:
                TOTAL=TOTAL+1
  	        KEY1="hulu"
                dict_hulu=dict()
                if ii.get("show_type")=='MO' or ii.get("show_type")=='SE':
                    print (ii.get("show_type"),ii.get("launch_id"))
                    arr_link_vudu=[]
                    dict_px=dict()
                    dict_hulu.setdefault(KEY1,[]) 
                    if ii.get("purchase_types")!=[]: 
                        for nn in ii.get("purchase_types"):
                            arr_link_vudu.append({"launch_id":ii.get("launch_id").encode('ascii','ignore'),"format":nn.get("format").encode('ascii','ignore'),"price":nn.get("price").encode('ascii','ignore'),"purchase_type":nn.get("type").encode('ascii','ignore')})
                        print arr_link_vudu 
                    else:
                        arr_link_vudu.append({"launch_id":ii.get("launch_id").encode('ascii','ignore'),"format":'sd',"price":'null',"purchase_type":'null',})               
                        print arr_link_vudu
                        
                    vudu_projectx_id=[]
                    if ii.get("show_type")=='MO':
                        url_vudu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=Showtime&showType=MO" %ii.get("launch_id")
                        response_vudu=urllib2.Request(url_vudu)
                        response_vudu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_vudu=urllib2.urlopen(response_vudu)
                        data_vudu=resp_vudu.read()
                        data_resp_vudu=json.loads(data_vudu)
                        for ii in data_resp_vudu:
                            if ii["data_source"]=='vudu' and ii["type"]=='Program' and ii["sub_type"]=='MO':
                                vudu_projectx_id.append(ii["projectx_id"])

                        if len(vudu_projectx_id)>1:
                            for mm in vudu_projectx_id:
                                if vudu_projectx_id.count(mm)>1:
                                    vudu_proejctx_id.remove(mm)
                        print("vudu_projectx_id", vudu_projectx_id)  
                    else:
                        url_vudu="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=Showtime&showType=SE" %ii.get("launch_id")
                        response_vudu=urllib2.Request(url_vudu)
                        response_vudu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_vudu=urllib2.urlopen(response_vudu)
                        data_vudu=resp_vudu.read()
                        data_resp_vudu=json.loads(data_vudu)
                        for ii in data_resp_vudu:
                            if ii["data_source"]=='vudu' and ii["type"]=='Program' and ii["sub_type"]=='SE':
                                vudu_projectx_id.append(ii["projectx_id"])
                        if len(vudu_projectx_id)>1:
                            for mm in vudu_projectx_id:
                                if vudu_projectx_id.count(mm)>1:
                                    vudu_proejctx_id.remove(mm)
                        print("vudu_projectx_id", vudu_projectx_id) 

                    if len(vudu_projectx_id)>1:
                        ids_present=['multiple mapped ids present']
                    else:
                        ids_present=['']

                    for y in vudu_projectx_id:
                        total=total+1
                        arr_link_px=[]
                        dict_px.setdefault(KEY1,[])
                        try:
                            url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%y
                            response_px=urllib2.Request(url_px)
                            response_px.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_px=urllib2.urlopen(response_px)
                            data_px=resp_px.read()
                            data_resp_px=json.loads(data_px)
                            for kk in data_resp_px:
                                for ll in kk["videos"]:
                                    if ll["source_id"]=="vudu" and ll["platform"]=='pc':
                                        arr_link_px.append({"launch_id":ll["source_program_id"].encode('ascii','ignore'),"format":ll["quality"].encode('ascii','ignore'),"price":ll["price"].encode('ascii','ignore'),"purchase_type":ll["purchase_type"].encode('ascii','ignore')})
                                    else:
                                        arr_link_px=[]
                            if len(arr_link_px)>1:
                                for mm in arr_link_px:
                                    if arr_link_px.count(mm)>1:
                                        arr_link_px.remove(mm) 

                            missing =[]
                            additional=[]

                            vudu=Counter(map(str,[j for j in arr_link_vudu]))
                            try:
                                px=Counter(map(str,[i for i in arr_link_px]))
                                e=vudu-px
                                if list(e.elements()):
                                    missing=missing.append(list(e.elements()))
                                f=px-vudu
                                if list(f.elements()):
                                    additional=additional.append(list(f.elements()))
                            except TypeError:
                                px=Counter()  
                                e=vudu-px
                                if list(e.elements()):
                                    missing=missing.append(list(e.elements()))
                                f=px-vudu
                                if list(f.elements()):
                                    additional=additional.append(list(f.elements()))

                            if missing==[] and additional==[]:
                                vudu_px=["Pass",'','No missing ott link',''] 
                                px_vudu=["Pass",'','No additional/duplicates ott link present']    

                            if missing and additional==[]:
                                expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%d&service_short_name=vudu"%ii.get("launch_id")
                                response_expired=urllib2.Request(expired_url)
                                response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                                resp_expired=urllib2.urlopen(response_expired)
                                data_expired=resp_expired.read()
                                data_resp_expired=json.loads(data_expired)
                                if data_resp_expired['is_available']==False:
                                    px_vudu=["Pass",'','No additional/duplicates ott link present']
                                    vudu_px=["Fail",missing,'missing ott link present','No']
                                if data_resp_expired['is_available']==True:
                                    px_vudu=["Pass",'','No additional/duplicates ott link present']
                                    vudu_px=["Pass",'','missing ott link present','expired']

                            if additional and missing==[]:
                                expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%d&service_short_name=vudu"%ii.get("launch_id")
                                response_expired=urllib2.Request(expired_url)
                                response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                                resp_expired=urllib2.urlopen(response_expired)
                                data_expired=resp_expired.read()
                                data_resp_expired=json.loads(data_expired)
                                if data_resp_expired['is_available']==False:
                                    additional=["Fail",additional,'additional/duplicates ott link present']
                                    missing=["Pass",'','No missing ott link present','No']
                                if data_resp_expired['is_available']==True:
                                    additional=["Fail",additional,'additional/duplicates ott link present']
                                    missing=["Pass",'','No missing ott link present','expired']

                            if missing and additional:
                                expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%d&service_short_name=vudu"%ii.get("launch_id")
                                response_expired=urllib2.Request(expired_url)
                                response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                                resp_expired=urllib2.urlopen(response_expired)
                                data_expired=resp_expired.read()
                                data_resp_expired=json.loads(data_expired)
                                if data_resp_expired['is_available']==False:
                                    additional=["Fail",additional,'additional/duplicates ott link present']
                                    missing=["Fail",missing,'missing ott link present','No']
                                if data_resp_expired['is_available']==True:
                                    additional=["Fail",additional,'additional/duplicates ott link present']
                                    missing=["Pass",missing,'missing ott link present','expired']

                            if ii.get("show_type")=='MO':
                                if missing[0]=='Pass' and additional[0]=='Pass':
                                    t=t+1
                                    writer.writerow({"vudu_id":ii.get("launch_id"),"title":ii.get("title").encode("ascii","ignore"),"show_type":ii.get("show_type").encode("ascii","ignore"),"release_year":ii.get("release_year").encode("ascii","ignore"),"episode_number":'',"season_number":'',"series_title":'',"vudu_link":ii.get("url").encode("ascii","ignore"),"projectx_id_vudu":str(y),"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":'',"Result":'Pass',"Comment":'Pass',"vudu Link expired":'',"Multiple Mapped ids":ids_present[0]})
                                if missing[0]=='Pass' and additional[0]=='Fail':
                                    h=h+1
                                    writer.writerow({"vudu_id":ii.get("launch_id"),"title":ii.get("title").encode("ascii","ignore"),"show_type":ii.get("show_type").encode("ascii","ignore"),"release_year":ii.get("release_year").encode("ascii","ignore"),"episode_number":'',"season_number":'',"series_title":'',"vudu_link":ii.get("url").encode("ascii","ignore"),"projectx_id_vudu":str(y),"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"Comment":additional[2],"vudu Link expired":missing[3],"Multiple Mapped ids":ids_present[0]})
                                if missing[0]=='Fail' and additional[0]=='Fail':
                                    v=v+1
                                    writer.writerow({"vudu_id":ii.get("launch_id"),"title":ii.get("title").encode("ascii","ignore"),"show_type":ii.get("show_type").encode("ascii","ignore"),"release_year":ii.get("release_year").encode("ascii","ignore"),"series_title":'',"vudu_link":ii.get("url").encode("ascii","ignore"),"projectx_id_vudu":str(y),"Link_present in PX API":'No',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"Comment":missing[2]+'and'+additional[2],"vudu Link expired":missing[3],"Multiple Mapped ids":ids_present[0]})
                                if missing[0]=='Fail' and additional[0]=='Pass':
                                    m=m+1
                                    writer.writerow({"vudu_id":ii.get("launch_id"),"title":ii.get("title").encode("ascii","ignore"),"show_type":ii.get("show_type").encode("ascii","ignore"),"release_year":ii.get("release_year").encode("ascii","ignore"),"series_title":'',"vudu_link":ii.get("url").encode("ascii","ignore"),"projectx_id_vudu":str(y),"Link_present in PX API":'No',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":'',"Result":'Fail',"Comment":missing[2],"vudu Link expired":missing[3],"Multiple Mapped ids":ids_present[0]})  


                            if ii.get("show_type")=='SE':
                                if missing[0]=='Pass' and additional[0]=='Pass':
                                    t=t+1
                                    print({"vudu_id":ii.get("launch_id"),"title":ii.get("title").encode("ascii","ignore"),"show_type":ii.get("show_type").encode("ascii","ignore"),"release_year":ii.get("release_year").encode("ascii","ignore"),"episode_number":ii.get("episode_number").encode("ascii","ignore"),"season_number":ii.get("season_number").encode("ascii","ignore"),"series_title":ii.get("series_title").encode("ascii","ignore"),"vudu_link":ii.get("url").encode("ascii","ignore"),"projectx_id_vudu":str(y),"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":'',"Result":'Pass',"Comment":'Pass',"vudu Link expired":'',"Multiple Mapped ids":ids_present[0]})
                                if missing[0]=='Pass' and additional[0]=='Fail':
                                    h=h+1
                                    writer.writerow({"vudu_id":ii.get("launch_id"),"title":ii.get("title").encode("ascii","ignore"),"show_type":ii.get("show_type").encode("ascii","ignore"),"release_year":ii.get("release_year").encode("ascii","ignore"),"episode_number":ii.get("episode_number").encode("ascii","ignore"),"season_number":ii.get("season_number").encode("ascii","ignore"),"series_title":ii.get("series_title").encode("ascii","ignore"),"vudu_link":ii.get("url").encode("ascii","ignore"),"projectx_id_vudu":str(y),"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"Comment":additional[2],"vudu Link expired":missing[3],"Multiple Mapped ids":ids_present[0]})
                                if missing[0]=='Fail' and additional[0]=='Fail':
                                    v=v+1
                                    writer.writerow({"vudu_id":ii.get("launch_id"),"title":ii.get("title").encode("ascii","ignore"),"show_type":ii.get("show_type").encode("ascii","ignore"),"release_year":ii.get("release_year").encode("ascii","ignore"),"episode_number":ii.get("episode_number").encode("ascii","ignore"),"season_number":ii.get("season_number").encode("ascii","ignore"),"series_title":ii.get("series_title").encode("ascii","ignore"),"vudu_link":ii.get("url").encode("ascii","ignore"),"projectx_id_vudu":str(y),"Link_present in PX API":'No',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"Comment":missing[2]+'and'+additional[2],"vudu Link expired":missing[3],"Multiple Mapped ids":ids_present[0]})
                                if missing[0]=='Fail' and additional[0]=='Pass':
                                    m=m+1
                                    writer.writerow({"vudu_id":ii.get("launch_id"),"title":ii.get("title").encode("ascii","ignore"),"show_type":ii.get("show_type").encode("ascii","ignore"),"release_year":ii.get("release_year").encode("ascii","ignore"),"episode_number":ii.get("episode_number").encode("ascii","ignore"),"season_number":ii.get("season_number").encode("ascii","ignore"),"series_title":ii.get("series_title").encode("ascii","ignore"),"vudu_link":ii.get("url").encode("ascii","ignore"),"projectx_id_vudu":str(y),"Link_present in PX API":'No',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":'',"Result":'Fail',"Comment":missing[2],"vudu Link expired":missing[3],"Multiple Mapped ids":ids_present[0]})    

                        except httplib.BadStatusLine:
                            continue
                        except urllib2.HTTPError:
                            continue
                        except socket.error:
                            continue

                    print ("thread_name:",name, "TOTAL:",TOTAL,"total ingested :",total)                        

    connection.close()

t1=threading.Thread(target=ott_link_checking_vudu,args=(0,"thread-1",501,1))
t1.start()

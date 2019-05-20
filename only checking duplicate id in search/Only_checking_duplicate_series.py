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

def duplicate_checking(px_id,gb_sm_title,writer,name):
    Token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
    domain_name="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com"
    token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
    try:
        search_px_id_=[]
        search_px_id=[]
        search_px_id__=[]
        search_px_id1_=[]
        search_px_id1=[]
        search_px_id_filtered=[]
        next_page_url=""
        duplicate="" 
        credit_match=''
        credit_array=[]
        data_resp_search=dict()
        #import pdb;pdb.set_trace()   
        print (px_id,gb_sm_title,writer,name)
        search_px_id=[]
        search_api="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/v3/voice_search?q=%s&safe_search=false&credit_summary=true&credit_types=Actor&aliases=true&ott=true"%urllib2.quote(gb_sm_title)
        response_search=urllib2.Request(search_api)
        response_search.add_header('User-Agent','Branch Fyra v1.0')
        response_search.add_header('Authorization',token) 
        resp_search=urllib2.urlopen(response_search)
        data_search=resp_search.read()
        data_resp_search=json.loads(data_search)
        if data_resp_search.get("top_results"):
            for ii in data_resp_search.get("top_results"):
                if ii.get("action_type")=="ott_search" and ii.get("action_type")!="web_results" and ii.get("results"):
                    for jj in ii.get("results"):
                        if jj.get("object").get("show_type")=='SM':
                            search_px_id.append(jj.get("object").get("id"))
                            search_px_id1=search_px_id1+search_px_id
                                                                                                                                                                                         
            if search_px_id:
                for mm in search_px_id:
                    if mm in px_id:
                        search_px_id_.append(mm)
                    else:
                        search_px_id_filtered.append(mm)



            if len(search_px_id_)==1 or search_px_id_==[]:
                try:
                    search_px_id1_.append(search_px_id_[0])
                    search_px_id_=[]
                    search_px_id=[]
                    duplicate='False'
                except IndexError:
                    search_px_id_=[]
                    search_px_id=[]
                    duplicate='False'
            else:
                if search_px_id_!=search_px_id__:
                    search_px_id__=search_px_id__+search_px_id_
                    duplicate='True'
                    search_px_id=[]
                else:
                    search_px_id__=search_px_id__
                    duplicate='True'
                    search_px_id=[] 

            if duplicate=='False': 
                while data_resp_search.get("results"):
                    for nn in data_resp_search.get("results"):
                        if nn.get("action_type")=="ott_search" and (nn.get("results")==[] or nn.get("results")):
                            next_page_url=nn.get("next_page_url")
                            if next_page_url is not None: 
                                search_api1=domain_name+next_page_url.replace(' ',"%20")
                                if search_api1!=domain_name :
                                    search_api=search_api1
                                    response_search=urllib2.Request(search_api)
                                    response_search.add_header('User-Agent','Branch Fyra v1.0')
                                    response_search.add_header('Authorization',token)
                                    resp_search=urllib2.urlopen(response_search)
                                    data_search=resp_search.read()
                                    data_resp_search=json.loads(data_search)
                            else:
                                data_resp_search={"resilts":[]}         
                        else:
                            data_resp_search={"resilts":[]}

                        if data_resp_search.get("results"):
                            for nn in data_resp_search.get('results'):
                                if nn.get("results"):
                                    for jj in nn.get("results"):
                                        if jj.get("object").get("show_type")=='SM':
                                            search_px_id.append(jj.get("object").get("id"))
                                            search_px_id1=search_px_id1+search_px_id  

                                if search_px_id:
                                    for mm in search_px_id:
                                        if mm in px_id:
                                            search_px_id_.append(mm)
                                        else:
                                            search_px_id_filtered.append(mm)
                        
                                for ss in search_px_id_:
                                    while search_px_id_.count(ss)>1:
                                        search_px_id_.remove(ss)

                                for ss in search_px_id1_:
                                    while search_px_id1_.count(ss)>1:
                                        search_px_id1_.remove(ss)
                                    while search_px_id1_.count(ss)>1:
                                        search_px_id1_.remove(ss) 

                                if len(search_px_id_)==1 or search_px_id_==[]:
                                    try:
                                        search_px_id1_.append(search_px_id_[0]) 
                                        search_px_id_=[] 
                                        search_px_id=[] 
                                        duplicate='False' 
                                    except IndexError:
                                        search_px_id_=[]
                                        search_px_id=[]
                                        duplicate='False'      
                                else:
                                    if search_px_id_!=search_px_id__:
                                        search_px_id__=search_px_id__+search_px_id_
                                        duplicate='True'
                                        search_px_id=[]
                                    else:
                                        search_px_id__=search_px_id__
                                        duplicate='True'
                                        search_px_id=[] 
        if search_px_id1:
            if len(search_px_id__)>1 and duplicate=='True':
                px_link="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true&aliases=true" %'{}'.format(",".join([str(i) for i in search_px_id__]))
                response_link=urllib2.Request(px_link)
                response_link.add_header('Authorization',token)
                resp_link=urllib2.urlopen(response_link)
                data_link=resp_link.read()
                data_resp_credits=json.loads(data_link)
                for uu in data_resp_credits:
                    if uu.get("credits"):
                        for tt in uu.get("credits"):
                            credit_array.append(unidecode.unidecode(tt.get("full_credit_name")))
                if credit_array:
                    for cc in credit_array:
                        if credit_array.count(cc)>1:
                            credit_match='True'
                            break
                        else:
                            credit_match='False'
                else:
                    credit_match='True'             
                if credit_match=="True": 
                    comment='Duplicate projectx ids found in search api'
                    writer.writerow([px_id,gb_sm_title,comment,comment,search_px_id__,credit_match])
                else:
                    comment='Duplicate projectx ids found in search api'
                    writer.writerow([px_id,gb_sm_title,comment,comment,search_px_id__,credit_match])                                       
            else:
                comment='Duplicate not found for series in search api' 
                writer.writerow([px_id,gb_sm_title,comment,comment,search_px_id__,credit_match])            
        else:
            comment='Search api has no response'
            writer.writerow([px_id,gb_sm_title,comment,comment,search_px_id__,credit_match])                              
       

    except httplib.BadStatusLine:
        print ("exception caught httplib.BadStatusLine................................................................................",name)
        print ("\n") 
        print ("Retrying.............")
        print ("\n") 
        duplicate_checking(px_id,gb_sm_title,writer,name)
    except urllib2.HTTPError:
        print ("exception caught HTTPError................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        duplicate_checking(px_id,gb_sm_title,writer,name)
    except socket.error:
        print ("exception caught SocketError..........................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        duplicate_checking(px_id,gb_sm_title,writer,name)   
    except URLError:
        print ("exception caught URLError.............................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        duplicate_checking(px_id,gb_sm_title,writer,name)
    except RuntimeError:
        print ("exception caught ..................................................................................",name,)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        duplicate_checking(px_id,gb_sm_title,writer,name)    	



def getting_ids(start,name,end,id):
    print name
    print("Checking duplicates of series and Movies to Projectx search api................")
    v=''
    inputFile="Projectx series mapping issues using Episode and series title"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/duplicate_checked_in_search_api%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with w as mycsvfile:
        fieldnames = ["px_id","GB_series_title","comment","Result",'Duplicate ids in search','Credit_match']

        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        gb_mo_list=[]
        gb_sm_list=[]

        for aa in range(start,end):
            #import pdb;pdb.set_trace()
            px_id=[]
            px_id.append(eval(fullist[aa][0]))
            px_id.append(eval(fullist[aa][1]))
            gb_sm_title=str(fullist[aa][2])
                     
            duplicate_checking(px_id,gb_sm_title,writer,name)
            

t1=threading.Thread(target=getting_ids,args=(1,"thread - 1",101,1))
t1.start()
t2=threading.Thread(target=getting_ids,args=(101,"thread - 2",201,2))
t2.start()
t3=threading.Thread(target=getting_ids,args=(201,"thread - 3",301,3))
t3.start()
t4=threading.Thread(target=getting_ids,args=(301,"thread - 4",467,4))
t4.start()
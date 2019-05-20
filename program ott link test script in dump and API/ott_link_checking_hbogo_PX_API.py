"""writer: Saayan"""

import httplib
import socket
import urllib2
import MySQLdb
import collections
import sys
import csv
import os
import pymysql
import datetime
from urllib2 import HTTPError
from collections import Counter
import json

def ott_link_checking_showtime():

    conn1=pymysql.connect(user="root",passwd="branch@123",host="localhost",db="branch_service")
    cur1=conn1.cursor()

    result_sheet='/hbonow_ott_link_checking_PX_API.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["hbonow_id","title","show_type","Release_year","season_number","episode_number","series_title","projectx_id_hbonow","Link_present in PX API","Missing ott_contents","Additional/Duplicates ott_contents","Result","Comment","hbogo link present","hbonow Link expired","Multiple Mapped ids"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        query1="select launch_id,show_type,title,url,release_year,season_number,episode_number,series_title from hbonow_programs where expired=0 and expired_at is NULL;"
        cur1.execute(query1)
        res1=cur1.fetchall()
        print res1
	TOTAL=0
	total=0
	total1=0
	total2=0
	KEY1="hbonow"
	KEY2="hbogo"
        for i in res1: 
	    TOTAL=TOTAL+1
	    arr_link_hbonow=[]
            dict1_=dict()
            dict1_.setdefault(KEY1,[])
	    arr_link_hbonow.append(i[0])
	    dict1_[KEY1].append(arr_link_hbonow[0])
	    print ("hbonow_id :", i[0])
            if i[1]=='MO':
                try:
		    hbonow_projectx_id=[]
		#    import pdb;pdb.set_trace()
                    url_hbonow="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=HBONOW&showType=MO" %i[0]
                    response_hbonow=urllib2.Request(url_hbonow)
                    response_hbonow.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_hbonow=urllib2.urlopen(response_hbonow)
                    data_hbonow=resp_hbonow.read()
                    data_resp_hbonow=json.loads(data_hbonow)
                
                    for ii in data_resp_hbonow:
		        if ii["data_source"]=='HBONOW' and ii["type"]=='Program' and ii["sub_type"]=='MO':
                            hbonow_projectx_id.append(ii["projectx_id"])
		    print("hbonow_projectx_id", hbonow_projectx_id)
                    if len(hbonow_projectx_id)>1:
			ids_present=['multiple mapped ids present']
		    else:
			ids_present=['']
                    for y in hbonow_projectx_id:
			total=total+1
                        arr_link_px=[]
			arr_link_px1=[]
			dict2_=dict()
                        url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%y
			response_px=urllib2.Request(url_px)
                        response_px.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_px=urllib2.urlopen(response_px)
                        data_px=resp_px.read()
                        data_resp_px=json.loads(data_px)

                        for kk in data_resp_px:
                            for ll in kk["videos"]:
                                if ll["source_id"]=="hbonow" and ll["platform"]=='pc':
				    arr_link_px.append(ll["launch_id"].encode())
                                if ll["source_id"]=="hbogo" and ll["platform"]=='pc':
				    arr_link_px1.append(ll["launch_id"].encode())     
                        if len(arr_link_px)>1:
                            for mm in arr_link_px:
				if arr_link_px.count(mm)>1:
				    arr_link_px.remove(mm)
			if len(arr_link_px1)>1:
                            for nn in arr_link_px1:
                                if arr_link_px1.count(nn)>1:
                                    arr_link_px1.remove(nn)

			comp1=''
			comp2=''
			dict3_=dict()
			if arr_link_px1!=[]:
                            if str(i[0]) in arr_link_px1:  
                                dict3_.setdefault(KEY2, [])
                                dict3_[KEY2].append(arr_link_px1)
				comp1=''
			    else:
                                dict3_.setdefault(KEY2, [])
                                dict3_[KEY2].append(arr_link_px1)
				comp1='additional'
			else:
			    dict3_.setdefault(KEY2, [])
                            dict3_[KEY2].append(arr_link_hbonow[0])
			    comp1='missing'

			if arr_link_px!=[]:
                            if str(i[0]) in arr_link_px:
                                dict2_.setdefault(KEY1, [])
                                dict2_[KEY1].append(arr_link_px)
				comp2=''
                            else:
                                dict2_.setdefault(KEY1, [])
                                dict2_[KEY1].append(arr_link_px)
				comp2='additional'
			else:
			    dict2_.setdefault(KEY1, [])
                            dict2_[KEY1].append(arr_link_hbonow[0])
			    comp2='missing'
	
		        additional=[]
		        missing=[]
                        if comp1=='' and comp2=='':
                            additional=["Pass",'','No additional/duplicates ott link present']
                            missing=["Pass",'','No missing ott link','']
                        if comp1=='additional' and comp2=='':
                            additional=["Fail",dict3_,'additional/duplicates ott link present for hbogo']
			    missing=["Pass",'',"No missing link present",'']
                        if comp1=='' and comp2=='additional':
			    expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
			    response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
			    if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No missing and dditional/duplicates ott link present']
				missng=["Pass","No missing link present",'expired']
			    if data_resp_expired['is_available']==False:
				missing=["Fail",dict1_,'missing ott link present','No']
				additional=["Fail",dict2_,'additional/duplicates ott link present for hbonow']
			if comp1=='missing' and comp2=='':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Fail",dict3_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
                        if comp1=='' and comp2=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
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
                                missing=["Pass",'','no missing ott link present','expired']
			if comp1=='missing' and comp2=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Fail",[dict3_,dict2_],'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']

			if comp1=='missing' and comp2=='additional':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",dict2_,'additional/duplicates ott link present']
                                missing=["Fail",dict3_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",dict2_,'additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
			if comp1=='additional' and comp2=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",dict3_,'additional/duplicates ott link present']
                                missing=["Fail",dict2_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",dict3_,'additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']

			if comp1=='additional' and comp2=='additional':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",[dict3_,dict2_],'additional/duplicates ott link present']
                                missing=["Fail",dict1_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",[dict3_,dict2_],'additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']

		        if missing[0]=='Pass' and additional[0]=='Pass':
               	  	    writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"projectx_id_hbonow":y,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":'',"Result":'Pass',"hbogo link present":dict3_,"hbonow Link expired":missing[3],"Comment":'Pass',"Multiple Mapped ids":ids_present[0]})
            	        if missing[0]=='Pass' and additional[0]=='Fail':
			    writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"projectx_id_hbonow":y,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hbogo link present":dict3_,"hbonow Link expired":missing[3],"Comment":'Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})
            	        if missing[0]=='Fail' and additional[0]=='Fail':
			    writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"projectx_id_hbonow":y,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hbogo link present":dict3_,"hbonow Link expired":missing[3],"Comment":'Missing and Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})
                        if missing[0]=='Fail' and additional[0]=='Pass':
		  	    writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"projectx_id_hbonow":y,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":'',"Result":'Fail',"hbogo link present":'',"hbonow Link expired":missing[3],"Comment":'Missing link present in api',"Multiple Mapped ids":ids_present[0]})
		    showtime_projectx_id=[]
		except httplib.BadStatusLine:
                    continue
                except urllib2.HTTPError:
                    continue
                except socket.error:
                    continue

            if i[1]=='episode':
		arr_link_px=[]
		arr_link_px1=[]
	        hbonow_projectx_id=[]
                try:
                    url_hbonow="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=HBONOW&showType=SE" %i[0]
                    response_hbonow=urllib2.Request(url_hbonow)
                    response_hbonow.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_hbonow=urllib2.urlopen(response_hbonow)
                    data_hbonow=resp_hbonow.read()
                    data_resp_hbonow=json.loads(data_hbonow)

                    for ii in data_resp_hbonow:
                        if ii["data_source"]=='HBONOW' and ii["type"]=='Program' and ii["sub_type"]=='SE':
                            hbonow_projectx_id.append(ii["projectx_id"])
		    print("hbonow_projectx_id", hbonow_projectx_id)
		    if len(hbonow_projectx_id)>1:
                        ids_present=['multiple mapped ids present']
                    else:
                        ids_present=['']
                    for jj in hbonow_projectx_id:
			total1=total1+1
                        arr_link_px=[]
                        arr_link_px1=[]
                        dict6_=dict()
                        url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%jj
                        response_px=urllib2.Request(url_px)
                        response_px.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_px=urllib2.urlopen(response_px)
                        data_px=resp_px.read()
                        data_resp_px=json.loads(data_px)
                        for kk in data_resp_px:
                            for ll in kk["videos"]:
                                if ll["source_id"]=="hbonow" and ll["platform"]=='pc':
                                    arr_link_px.append(ll["launch_id"].encode())
                                if ll["source_id"]=="hbogo" and ll["platform"]=='pc':
                                    arr_link_px1.append(ll["launch_id"].encode())
                        if len(arr_link_px)>1:
                            for mm in arr_link_px:
                                if arr_link_px.count(mm)>1:
                                    arr_link_px.remove(mm)
			if len(arr_link_px1)>1:
                            for nn in arr_link_px1:
                                if arr_link_px1.count(nn)>1:
                                    arr_link_px1.remove(nn)

			comp3=''
                        comp4=''
                        dict7_=dict()
                        dict8_=dict()
                        dict9_=dict()
                        if arr_link_px1!=[]:
                            if str(i[0]) in arr_link_px1:
                                dict7_.setdefault(KEY2, [])
                                dict7_[KEY2].append(arr_link_px1)
				comp3=''
			    else:
                                dict7_.setdefault(KEY2, [])
                                dict7_[KEY2].append(arr_link_px1)
				comp3='additional'
			else:
			    dict7_.setdefault(KEY2, [])
                            dict7_[KEY2].append(arr_link_hbonow[0])
			    comp3='missing'
			
			if arr_link_px!=[]:
                            if str(i[0]) in arr_link_px:
                                dict6_.setdefault(KEY1, [])
                                dict6_[KEY1].append(arr_link_px)
				comp4=''
                            else:
                                dict6_.setdefault(KEY1, [])
                                dict6_[KEY1].append(arr_link_px)
				comp4='additional'
		        else:
			    dict6_.setdefault(KEY1, [])
                            dict6_[KEY1].append(arr_link_hbonow[0])
			    comp4='missing'	

			additional=[]
                        missing=[]
                        if comp3=='' and comp4=='':
                            additional=["Pass",'','No additional/duplicates ott link present']
                            missing=["Pass",'','No missing ott link','']
                        if comp3=='additional' and comp4=='':
                            additional=["Fail",dict7_,'additional/duplicates ott link present for hbogo']
                            missing=["Pass",'',"No missing link present",'']
                        if comp3=='' and comp4=='additional':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No missing and dditional/duplicates ott link present']
                                missng=["Pass","No missing link present",'expired']
                            if data_resp_expired['is_available']==False:
                                missing=["Fail",dict1_,'missing ott link present','No']
                                additional=["Fail",dict6_,'additional/duplicates ott link present for hbonow']

			if comp3=='missing' and comp4=='':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Fail",dict7_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
                        if comp3=='' and comp4=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Fail",dict6_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
			if comp3=='missing' and comp4=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Fail",[dict6_,dict7_],'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']

			if comp3=='missing' and comp4=='additional':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",dict6_,'additional/duplicates ott link present']
                                missing=["Fail",dict7_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",dict6_,'additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
			if comp3=='additional' and comp4=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",dict7_,'additional/duplicates ott link present']
                                missing=["Fail",dict6_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",dict7_,'additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']	
			if comp3=='additional' and comp4=='additional':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",[dict7_,dict6_],'additional/duplicates ott link present']
                                missing=["Fail",dict1_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",[dict7_,dict6_],'additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']

		        if missing[0]=='Pass' and additional[0]=='Pass':
                            writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"season_number":i[5],"episode_number":i[6],"series_title":i[7],"projectx_id_hbonow":jj,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":'',"Result":'Pass',"hbogo link present":dict7_,"hbonow Link expired":missing[3],"Comment":'Pass',"Multiple Mapped ids":ids_present[0]})
                        if missing[0]=='Pass' and additional[0]=='Fail':
                            writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"season_number":i[5],"episode_number":i[6],"series_title":i[7],"projectx_id_hbonow":jj,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hbogo link present":dict7_,"hbonow Link expired":missing[3],"Comment":'Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})
                        if missing[0]=='Fail' and additional[0]=='Fail':
                            writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"season_number":i[5],"episode_number":i[6],"series_title":i[7],"projectx_id_hbonow":jj,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hbogo link present":dict7_,"hbonow Link expired":missing[3],"Comment":'Missing and Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})
                        if missing[0]=='Fail' and additional[0]=='Pass':
                            writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"season_number":i[5],"episode_number":i[6],"series_title":i[7],"projectx_id_hbonow":jj,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":'',"Result":'Fail',"hbogo link present":'',"hbonow Link expired":missing[3],"Comment":'Missing link present in api',"Multiple Mapped ids":ids_present[0]})
                except httplib.BadStatusLine:
                    continue
                except urllib2.HTTPError:
                    continue
                except socket.error:
                    continue

            if i[1]=='tv_show':
                arr_link_px=[]
                arr_link_px1=[]
		hbonow_projectx_id=[]
                try:
                    url_hbonow="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=HBONOW&showType=SM" %i[0]
                    response_hbonow=urllib2.Request(url_showtime1)
                    response_hbonow.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_hbonow=urllib2.urlopen(response_hbonow)
                    data_hbonow=resp_hbonow.read()
                    data_resp_hbonow=json.loads(data_hbonow)

                    for ii in data_resp_hbonow:
                        if ii["data_source"]=='HBONOW' and ii["type"]=='Program' and ii["sub_type"]=='SM':
                            hbonow_projectx_id.append(ii["projectx_id"])
                    print("hbonow_projectx_id", hbonow_projectx_id)
		    if len(hbonow_projectx_id)>1:
                        ids_present=['multiple mapped ids present']
                    else:
                        ids_present=['']
                    for jj in hbonow_projectx_id:
			total2=total2+1
                        arr_link_px=[]
                        arr_link_px1=[]
                        dict10_=dict()
                        url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%jj
                        response_px=urllib2.Request(url_px)
                        response_px.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_px=urllib2.urlopen(response_px)
                        data_px=resp_px.read()
                        data_resp_px=json.loads(data_px)
                        for kk in data_resp_px:
                            for ll in kk["videos"]:
                                if ll["source_id"]=="hbonow" and ll["platform"]=='pc':
                                    arr_link_px.append(ll["launch_id"].encode())
                                if ll["source_id"]=="hbogo" and ll["platform"]=='pc':
                                    arr_link_px1.append(ll["launch_id"].encode())
                        if len(arr_link_px)>1:
                            for mm in arr_link_px:
                                if arr_link_px.count(mm)>1:
                                    arr_link_px.remove(mm)
			if len(arr_link_px1)>1:
                            for nn in arr_link_px1:
                                if arr_link_px1.count(nn)>1:
                                    arr_link_px1.remove(nn)

                        comp5=''
                        comp6=''
                        dict11_=dict()
                        dict12_=dict()
			dict13_=dict()
                        if arr_link_px1!=[]:
                            if str(i[0]) in arr_link_px1:
                                dict11_.setdefault(KEY2, [])
                                dict11_[KEY2].append(arr_link_px1)
				comp5=''
                            else:
                                dict11_.setdefault(KEY2, [])
                                dict11_[KEY2].append(arr_link_px1)
				comp5='additional'
			else:
			    dict11_.setdefault(KEY2, [])
                            dict11_[KEY2].append(arr_link_hbonow[0])
			    comp5='missing'

			if arr_link_px!=[]:
                            if str(i[0]) in arr_link_px:
                                dict10_.setdefault(KEY1, [])
                                dict10_[KEY1].append(arr_link_px)
				comp6=''
                            else:
                                dict10_.setdefault(KEY1, [])
                                dict10_[KEY1].append(arr_link_px)
				comp6='additional'
			else:
			    dict10_.setdefault(KEY1, [])
                            dict10_[KEY1].append(arr_link_hbonow[0])
			    comp6='missing'

			additional=[]
                        missing=[]
                        if comp5=='' and comp6=='':
                            additional=["Pass",'','No additional/duplicates ott link present']
                            missing=["Pass",'','No missing ott link','']
                        if comp5=='additional' and comp6=='':
                            additional=["Fail",dict11_,'additional/duplicates ott link present for hbogo']
                            missing=["Pass",'',"No missing link present",'']
                        if comp5=='' and comp6=='additional':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No missing and dditional/duplicates ott link present']
                                missng=["Pass","No missing link present",'expired']
                            if data_resp_expired['is_available']==False:
                                missing=["Fail",dict1_,'missing ott link present','No']
                                additional=["Fail",dict10_,'additional/duplicates ott link present for hbonow']
                        if comp5=='missing' and comp6=='':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Fail",dict11_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
			if comp5=='' and comp6=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Fail",dict10_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
                        if comp5=='missing' and comp6=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Fail",[dict10_,dict11_],'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Pass",'','No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
			if comp5=='missing' and comp6=='additional':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",dict10_,'No additional/duplicates ott link present']
                                missing=["Fail",dict11_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",dict10_,'No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
			if comp5=='additional' and comp6=='missing':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",dict11_,'No additional/duplicates ott link present']
                                missing=["Fail",dict10_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",dict11_,'No additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']
			if comp3=='additional' and comp4=='additional':
                            expired_url="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=showtime"%i[0]
                            response_expired=urllib2.Request(expired_url)
                            response_expired.add_header('Authorization','Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7')
                            resp_expired=urllib2.urlopen(response_expired)
                            data_expired=resp_expired.read()
                            data_resp_expired=json.loads(data_expired)
                            if data_resp_expired['is_available']==False:
                                additional=["Fail",[dict10_,dict11_],'additional/duplicates ott link present']
                                missing=["Fail",dict1_,'missing ott link present','No']
                            if data_resp_expired['is_available']==True:
                                additional=["Fail",[dict10_,dict11_],'additional/duplicates ott link present']
                                missing=["Pass",'','no missing ott link present','expired']

	   	        if missing[0]=='Pass' and additional[0]=='Pass':
                            writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"season_number":i[5],"episode_number":i[6],"series_title":i[7],"projectx_id_hbonow":jj,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":'',"Result":'Pass',"hbogo link present":dict11_,"hbonow Link expired":missing[3],"Comment":'Pass',"Multiple Mapped ids":ids_present[0]})
                        if missing[0]=='Pass' and additional[0]=='Fail':
                            writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"season_number":i[5],"episode_number":i[6],"series_title":i[7],"projectx_id_hbonow":jj,"Link_present in PX API":'Yes',"Missing ott_contents":'',"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hbogo link present":dict11_,"hbonow Link expired":missing[3],"Comment":'Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})
                        if missing[0]=='Fail' and additional[0]=='Fail':
                            writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"season_number":i[5],"episode_number":i[6],"series_title":i[7],"projectx_id_hbonow":jj,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":additional[1],"Result":'Fail',"hbogo link present":dict11_,"hbonow Link expired":missing[3],"Comment":'Missing and Additional/duplicates link present in api',"Multiple Mapped ids":ids_present[0]})
                        if missing[0]=='Fail' and additional[0]=='Pass':
                            writer.writerow({"hbonow_id":i[0],"title":i[2],"show_type":i[1],"Release_year":i[4],"season_number":i[5],"episode_number":i[6],"series_title":i[7],"projectx_id_hbonow":jj,"Link_present in PX API":'NO',"Missing ott_contents":missing[1],"Additional/Duplicates ott_contents":'',"Result":'Fail',"hbogo link present":'',"hbonow Link expired":missing[3],"Comment":'Missing link present in api',"Multiple Mapped ids":ids_present[0]})
                except httplib.BadStatusLine:
                    continue
                except urllib2.HTTPError:
                    continue
                except socket.error:
                    continue
	    print TOTAL
            print("total:",total+total1+total2)
        print datetime.datetime.now()    

ott_link_checking_showtime()

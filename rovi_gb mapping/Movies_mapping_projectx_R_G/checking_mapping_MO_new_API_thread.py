"""Writer: Saayan"""

import threading
import MySQLdb
import collections
from pprint import pprint
import sys
import urllib2
import json
import os
from urllib2 import HTTPError
import csv
import urllib
import os
import pymysql
import datetime
import httplib
import socket
from urllib2 import URLError


def open_csv(start,name,end,id):
    inputFile="GB_Rovi_Mapping_Movies"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_mapping_MO_new_API%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["Gb_id","Rovi_id","movie_name","release_year","projectx_id_rovi","projectx_id_gb","Dev_gb_px_mapped","Dev_rovi_px_mapped","another_rovi_id_present","another_gb_id_present","variant_present","Comment","Result of mapping"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        j=0
        k=0
        l=0
        m=0
	n=0
        p=0
        gg=0 
        total=0
        Token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        for r in range(start,end):
	    gb_projectx_id=[]
	    rovi_projectx_id=[]
            dev_rovi_px_mapped=[]
            dev_gb_px_mapped=[]
            px_series_id=[]
            px_array=[]
            px_variant_id=[]
            variant_present='False' 
         
            another_gb_id_present=''
            another_rovi_id_present=''
            total=total+1
            gb_id=str(fullist[r][0])
            movie_name=str(fullist[r][1])
            release_year=str(fullist[r][2])
            rovi_id=str(fullist[r][3])
#            import pdb;pdb.set_trace()
 
	    try:
	        url_rovi="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi"%eval(rovi_id)
	        response_rovi=urllib2.Request(url_rovi)
                response_rovi.add_header('Authorization',Token)
                resp_rovi=urllib2.urlopen(response_rovi)
                data_rovi=resp_rovi.read()
                data_resp_rovi=json.loads(data_rovi)

	        url_gb="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=MO"%eval(gb_id)
                response_gb=urllib2.Request(url_gb)
                response_gb.add_header('Authorization',Token)
                resp_gb=urllib2.urlopen(response_gb)
                data_gb=resp_gb.read()
                data_resp_gb=json.loads(data_gb)
	    except httplib.BadStatusLine:
	        print ("exception caught httplib.BadStatusLine............................................")
	        continue
            except urllib2.HTTPError:
                print ("exception caught urllib2.HTTPError............................................")  
                continue
            except socket.error:
                print ("exception caught socket.error............................................") 
                continue
            except URLError:
                print ("exception caught URLError ............................................")
                continue	    


      	    for ii in data_resp_rovi:
		if ii["data_source"]=='Rovi' and ii["type"]=='Program':
		    rovi_projectx_id.append(ii["projectx_id"])
	    for jj in data_resp_gb:
		if jj["data_source"]=='GuideBox' and jj["type"]=='Program' and jj["sub_type"]=='MO':
                    gb_projectx_id.append(jj["projectx_id"])

            print gb_projectx_id
            print rovi_projectx_id
            
            if len(rovi_projectx_id)>1 or len(gb_projectx_id)>1:
                if gb_projectx_id and rovi_projectx_id:
                    j=j+1
                    a=gb_projectx_id
                    b=rovi_projectx_id
		    status='Nil'
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(gb_projectx_id),"Result of mapping":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(gb_projectx_id),"Result of mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})

                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Result of mapping":'Pass',"Comment":'Multiple ingestion for same content of GB'})
                        if status=='Nil':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Result of mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of GB'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Multiple ingestion for same content of both sources',"Result of mapping":'Pass'})
                        if status=='Nil':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Fail:Multiple ingestion for same content of both sources',"Result of mapping":'Fail'})

            if len(rovi_projectx_id)==1 and len(gb_projectx_id)==1:
                if rovi_projectx_id == gb_projectx_id:
                    k=k+1
                    writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":str(gb_projectx_id[0]),"Comment":'Pass',"Result of mapping":'Pass'})

                if rovi_projectx_id != gb_projectx_id:
                    l=l+1
#                    import pdb;pdb.set_trace()
                    projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping'%rovi_projectx_id[0]
                    mapping_link=urllib2.Request(projectx_mapping_api)
                    mapping_link.add_header('Authorization',Token)
                    mapping_resp=urllib2.urlopen(mapping_link)
                    data_mapped=mapping_resp.read()
                    data_mapped_resp=json.loads(data_mapped)
                    for mapped in data_mapped_resp:
                        if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
                            dev_rovi_px_mapped.append({str(rovi_projectx_id[0]):"rovi_id:"+str(mapped.get("source_id")+','+'Map_reason:'+str(mapped.get("map_reason")))})

                        if mapped.get("type")=="Program" and mapped.get("data_source")=='GuideBox' and mapped.get("sub_type")=='MO':
                            dev_rovi_px_mapped.append({"gb_id":str(mapped.get("source_id")+','+'Map_reason:'+str(mapped.get("map_reason")))})
                            if mapped.get("source_id")!=str(gb_id):
                                another_gb_id_present='True'
                            else:
                                another_gb_id_present='False'

                    projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping'%gb_projectx_id[0]
                    mapping_link=urllib2.Request(projectx_mapping_api)
                    mapping_link.add_header('Authorization',Token)
                    mapping_resp=urllib2.urlopen(mapping_link)
                    data_mapped=mapping_resp.read()
                    data_mapped_resp1=json.loads(data_mapped)
                    for mapped in data_mapped_resp1:
                        if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
                            dev_gb_px_mapped.append({str(rovi_projectx_id[0]):"rovi_id:"+str(mapped.get("source_id")+','+'Map_reason:'+str(mapped.get("map_reason")))})
                            if mapped.get("source_id")!=str(rovi_id):
                                another_rovi_id_present='True'
                            else:
                                another_rovi_id_present='False'

                        if mapped.get("type")=="Program" and mapped.get("data_source")=='GuideBox' and mapped.get("sub_type")=='MO':
                            dev_gb_px_mapped.append({"gb_id":str(mapped.get("source_id")+','+'Map_reason:'+str(mapped.get("map_reason")))})

                    px_array=[gb_projectx_id[0],rovi_projectx_id[0]]
                    projectx_api="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true&aliases=true"%'{}'.format(",".join([str(i) for i in px_array]))
                    projectx_api_details=urllib2.Request(projectx_api)
                    projectx_api_details.add_header('Authorization',Token)
                    projectx_api_resp=urllib2.urlopen(projectx_api_details)
                    projectx_api_data_resp=projectx_api_resp.read()
                    projectx_api_response=json.loads(projectx_api_data_resp)
                    for kk in projectx_api_response:
                        if kk.get("variant_parent_id") is not None:  
                            px_variant_id.append(kk.get("variant_parent_id"))
                    for id_ in px_array:
                        if id_ in px_variant_id:
                            variant_present='True'
                            break
                                   
                    writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":str(gb_projectx_id[0]),"Dev_gb_px_mapped":dev_gb_px_mapped,"Dev_rovi_px_mapped":dev_rovi_px_mapped,"another_rovi_id_present":another_rovi_id_present,"another_gb_id_present":another_gb_id_present,"variant_present":variant_present,"Comment":'Fail',"Result of mapping":'Fail'})

            if gb_projectx_id and not rovi_projectx_id:
                m=m+1
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":'Nil',"projectx_id_gb":str(gb_projectx_id),"Comment":'No ingestion of rovi_id',"Result of mapping":'Fail'})

            if rovi_projectx_id and not gb_projectx_id:
                try:
#                    import pdb;pdb.set_trace()
                    duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=MO"%eval(gb_id)
                    response_url=urllib2.Request(duplicate_api)
                    response_url.add_header('Authorization',Token)
                    resp_url=urllib2.urlopen(response_url)
                    data_url=resp_url.read()
                    data_resp_url=json.loads(data_url)
                    if data_resp_url==[]:
                        id_api="http://34.231.212.186:81/projectx/guidebox?sourceId=%d&showType=MO"%eval(gb_id)
                        response_url1=urllib2.Request(id_api)
                        response_url1.add_header('Authorization',Token)
                        resp_url1=urllib2.urlopen(response_url1)
                        data_url1=resp_url1.read()
                        data_resp_url1=json.loads(data_url1)
                        if data_resp_url1==True:
                            n=n+1
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":'Nil',"Comment":'No ingestion of gb_id',"Result of mapping":'Fail'})
                        else:
                            gg=gg+1
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(gb_projectx_id),"Comment":'GB id not present in DB',"Result of mapping":'Fail'}) 
                    else:
                        while len(data_resp_url)>1: 
                            for i in data_resp_url:
                                if data_resp_url.count(i)>1:
                                    data_resp_url.remove(i)
                            for i in data_resp_url:
                                if data_resp_url.count(i)>1:
                                    data_resp_url.remove(i)
                      
                        if len(data_resp_url)==1:
                            for ll in data_resp_url:
                                gb_projectx_id.append(ll.get("projectx_id"))
                        if rovi_projectx_id==gb_projectx_id:
                            k=k+1
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(gb_projectx_id),"Comment":'Pass',"Result of mapping":'Pass'})
                        else:
                            l=l+1
                            projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping'%rovi_projectx_id[0]
                            mapping_link=urllib2.Request(projectx_mapping_api)
                            mapping_link.add_header('Authorization',Token)
                            mapping_resp=urllib2.urlopen(mapping_link)
                            data_mapped=mapping_resp.read()
                            data_mapped_resp=json.loads(data_mapped)
                            for mapped in data_mapped_resp:
                                if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
                                    dev_rovi_px_mapped.append({str(rovi_projectx_id[0]):"rovi_id:"+str(mapped.get("source_id")+','+'Map_reason:'+str(mapped.get("map_reason")))})

                                if mapped.get("type")=="Program" and mapped.get("data_source")=='GuideBox' and mapped.get("sub_type")=='MO':
                                    dev_rovi_px_mapped.append({"gb_id":str(mapped.get("source_id")+','+'Map_reason:'+str(mapped.get("map_reason")))})
                                    if mapped.get("source_id")!=str(gb_id):
                                        another_gb_id_present='True'
                                    else:
                                        another_gb_id_present='False'

                            projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping'%gb_projectx_id[0]
                            mapping_link=urllib2.Request(projectx_mapping_api)
                            mapping_link.add_header('Authorization',Token)
                            mapping_resp=urllib2.urlopen(mapping_link)
                            data_mapped=mapping_resp.read()
                            data_mapped_resp1=json.loads(data_mapped)
                            for mapped in data_mapped_resp1:
                                if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
                                    dev_gb_px_mapped.append({str(rovi_projectx_id[0]):"rovi_id:"+str(mapped.get("source_id")+','+'Map_reason:'+str(mapped.get("map_reason")))})
                                    if mapped.get("source_id")!=str(rovi_id):
                                        another_rovi_id_present='True'
                                    else:
                                        another_rovi_id_present='False'

                                if mapped.get("type")=="Program" and mapped.get("data_source")=='GuideBox' and mapped.get("sub_type")=='MO':
                                    dev_gb_px_mapped.append({"gb_id":str(mapped.get("source_id")+','+'Map_reason:'+str(mapped.get("map_reason")))})

                            px_array=[gb_projectx_id[0],rovi_projectx_id[0]]
                            projectx_api="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true&aliases=true"%'{}'.format(",".join([str(i) for i in px_array]))
                            projectx_api_details=urllib2.Request(projectx_api)
                            projectx_api_details.add_header('Authorization',Token)
                            projectx_api_resp=urllib2.urlopen(projectx_api_details)
                            projectx_api_data_resp=projectx_api_resp.read()
                            projectx_api_response=json.loads(projectx_api_data_resp)
                            for kk in projectx_api_response:
                                if kk.get("variant_parent_id") is not None:
                                    px_variant_id.append(kk.get("variant_parent_id"))
                            for id_ in px_array:
                                if id_ in px_variant_id:
                                    variant_present='True'
                                    break 

                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(gb_projectx_id),"Dev_gb_px_mapped":dev_gb_px_mapped,"Dev_rovi_px_mapped":dev_rovi_px_mapped,"another_rovi_id_present":another_rovi_id_present,"another_gb_id_present":another_gb_id_present,"variant_present":variant_present,"Comment":'Fail',"Result of mapping":'Fail'})

                except httplib.BadStatusLine:
                    print ("exception caught httplib.BadStatusLine...........................................")
                    continue
                except urllib2.HTTPError:
                    print ("exception caught urllib2.HTTPError............................................")
                    continue
                except socket.error:
                    print ("exception caught socket.error............................................")
                    continue
                except URLError:
                    print ("exception caught URLError ............................................")
                    continue

            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_gb":'',"Comment":'No Ingestion',"Result of mapping":'N.A'})
	    print("thread name:",name,"total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "gb Id not ingested : ",n, "both not ingested: ",p,"program not present in DB", gg)
    print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "gb Id not ingested : ",n, "both not ingested: ",p,"program not present in DB", gg)
    print(datetime.datetime.now())


t1=threading.Thread(target=open_csv,args=(1,'Thread -1',10001,1))
t1.start()
t2=threading.Thread(target=open_csv,args=(10001,'Thread -2',20001,2))
t2.start()
t3=threading.Thread(target=open_csv,args=(20001,'Thread -3',25798,3))
t3.start()

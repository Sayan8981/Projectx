"""Writer: Saayan"""

import threading
import socket
import MySQLdb
import collections
from pprint import pprint
import sys
import urllib2
import json
import os
from urllib2 import HTTPError
from urllib2 import URLError
import urllib
import csv
import os
import pymysql
import datetime
import httplib

def open_csv(start,name,end,id):
    
    inputFile="GB_TvShow_Rovi"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_mapping_Episodes_API%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["Gb_sm_id","Gb_id","Rovi_id","episode_title","ozoneepisodetitle","OzoneOriginalEpisodeTitle","projectx_id_rovi","projectx_id_gb","Episode mapping","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        j=0
        k=0
        l=0
        m=0
        n=0
        p=0
        q=0
        z=0
        gg=0
        total=0
        for r in range(start,end):
            print ({"start":start,"end":end})
	    gb_projectx_id=[]
	    rovi_projectx_id=[]
            gb_projectx_id_sm=[]
            total=total+1
            gb_id=eval(fullist[r][4])
            rovi_id=eval(fullist[r][10])
            print ("gb_id",gb_id)
            print ("rovi_id",rovi_id)
            gb_sm_id=int(fullist[r][0])
            print ("gb_sm_id",gb_sm_id)
            episode_title=str(fullist[r][7])
	    ozoneepisodetitle=str(fullist[r][9])
	    OzoneOriginalEpisodeTitle=str(fullist[r][8]) 

	    try:
	        url_rovi="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi" %rovi_id
                response_rovi=urllib2.Request(url_rovi)
                response_rovi.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_rovi=urllib2.urlopen(response_rovi)
                data_rovi=resp_rovi.read()
                data_resp_rovi=json.loads(data_rovi)

	        url_gb="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=GuideBox&showType=SE" %gb_id
                response_gb=urllib2.Request(url_gb)
                response_gb.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_gb=urllib2.urlopen(response_gb)
                data_gb=resp_gb.read()
                data_resp_gb=json.loads(data_gb)
	    except httplib.BadStatusLine:
                print ("exception caught httplib.BadStatusLine...........................................")
		continue
	    except urllib2.HTTPError:
                print ("exception caught urllib2.HTTPError............................................") 
		continue
	    except socket.error:
                print ("exception caught socket.error............................................") 
		continue
            except urllib2.URLError:
                print ("exception caught URLerror............................................")
                continue

	    for ii in data_resp_rovi:
                if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                    rovi_projectx_id.append(ii["projectx_id"])
            for jj in data_resp_gb:
                if jj["data_source"]=="GuideBox" and jj["type"]=="Program" and jj["sub_type"]=="SE":
                    gb_projectx_id.append(jj["projectx_id"])	
            print gb_projectx_id
            print rovi_projectx_id
  
            if len(rovi_projectx_id)==1 and len(gb_projectx_id)==1:
                if rovi_projectx_id == gb_projectx_id:
                    j=j+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":str(gb_projectx_id[0]),"Episode mapping":'Pass',"Comment":'Pass'})
                if rovi_projectx_id != gb_projectx_id:
                    k=k+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":str(gb_projectx_id[0]),"Episode mapping":'Fail',"Comment":'Fail'})
            
	    if len(rovi_projectx_id)>1 or len(gb_projectx_id)>1:
                if gb_projectx_id and rovi_projectx_id:
                    l+l+1
                    a=gb_projectx_id
                    b=rovi_projectx_id
                    status='Nil'
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Episode mapping":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi": rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Episode mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})
                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Episode mapping":'Pass',"Comment":'Multiple ingestion for same content of GB'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Episode mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of GB'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b: 
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Multiple ingestion for same content of both sources',"Episode mapping":'Pass'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Fail:Multiple ingestion for same content of both sources',"Episode mapping":'Fail'})

                if gb_projectx_id and not rovi_projectx_id:
                    m=m+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'Nil',"projectx_id_gb":gb_projectx_id,"Episode mapping":'N.A',"Comment":'No ingestion of rovi_id of episodes and multiple ingestion for GB_id'})

                if rovi_projectx_id and not gb_projectx_id:
                    n=n+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":'Nil',"Episode mapping":'N.A',"Comment":'No ingestion of gb_id and multiple ingestion for Rovi_id'})

            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'',"projectx_id_gb":'',"Episode mapping":'N.A',"Comment":'No Ingestion of both sources'})

            if len(gb_projectx_id)==1 and len(rovi_projectx_id)==0:
                q=q+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'',"projectx_id_gb":str(gb_projectx_id[0]),"Episode mapping":'Fail',"Comment":'No Ingestion of rovi_id of episode'})

            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==1:
                try:
                    duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=SM"%gb_sm_id
                    response_url=urllib2.Request(duplicate_api)
                    response_url.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_url=urllib2.urlopen(response_url)
                    data_url=resp_url.read()
                    data_resp_url=json.loads(data_url)
                    if data_resp_url==[]:
                        id_api="http://34.231.212.186:81/projectx/guidebox?sourceId=%d&showType=SM"%gb_sm_id
                        response_url1=urllib2.Request(id_api)
                        response_url1.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_url1=urllib2.urlopen(response_url1)
                        data_url1=resp_url1.read()
                        data_resp_url1=json.loads(data_url1)
                        if data_resp_url1==True:
                            z=z+1 
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":'',"Episode mapping":'Fail',"Comment":'No Ingestion of gb_id of episode'})
                        else:
                            gg=gg+1
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":'',"Episode mapping":'Fail',"Comment":'Not present of gb_id of series in GB DB'})  
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
                                gb_projectx_id_sm.append(ll.get("projectx_id"))
                        projectx_url="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d/episodes?&ott=true"%gb_projectx_id_sm[0]
                        response_url=urllib2.Request(projectx_url)
                        response_url.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_url=urllib2.urlopen(response_url)
                        data_url=resp_url.read()
                        data_resp_url=json.loads(data_url)
                        if data_resp_url!=[]:
                            for hh in data_resp_url:
                                gb_projectx_id.append(hh.get("id"))
                        else:
                            z=z+1 
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_gb":'',"Episode mapping":'Fail',"Comment":'No Ingestion of gb_id of episode'})
                       
                        if gb_projectx_id!=[]:
                            if rovi_projectx_id[0] in gb_projectx_id:
                                j=j+1
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(rovi_projectx_id),"Episode mapping":'Pass',"Comment":'Pass'})
                            else: 
                                k=k+1
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_gb":str(gb_projectx_id),"Episode mapping":'Fail',"Comment":'Fail'})       

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
                    print ("exception caught URLerror............................................")
                    continue
	    print("thread name:",name, "total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "gb Id not ingested : ",n+z, "both not ingested: ",p, "id not present in db:" ,gg)

    print("total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "gb Id not ingested : ",n+r, "both not ingested: ",p, "id not present in db:" ,gg)
    print(datetime.datetime.now())

t1 =threading.Thread(target=open_csv,args=(1,"thread - 1",30001,1))
t1.start()
t2 =threading.Thread(target=open_csv,args=(30001,"thread - 2",60001,2))
t2.start()
t3 =threading.Thread(target=open_csv,args=(60001,"thread - 3",90001,3))
t3.start()
t4 =threading.Thread(target=open_csv,args=(90001,"thread - 4",120001,4))
t4.start()
t5 =threading.Thread(target=open_csv,args=(120001,"thread - 5",150001,5))
t5.start()
t6 =threading.Thread(target=open_csv,args=(150001,"thread - 6",175953,6))
t6.start()



import pymongo
from pprint import pprint
import sys
import os
import csv
import pymysql
import collections
from pprint import pprint
import MySQLdb
from collections import Counter
import datetime
import urllib2
import json
import os
from urllib2 import HTTPError
import urllib
import socket
import httplib
from urllib2 import URLError
sys.setrecursionlimit(20000)
import unidecode
import threading
import re
import unidecode
from fuzzywuzzy import fuzz
from fuzzywuzzy import process



def credits_checking(start,name,end,id):
    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["ozone"]
    mytable=mydb["guidebox_program_details"]
    conn=pymysql.connect(user="branch",passwd="(branch)",host="192.168.86.7",db="branch_service_rovi")
    cur=conn.cursor()
         
    inputFile="GB_Movie_Rovi"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/credit_checked_Projectx_Api%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    v=open(os.getcwd()+result_sheet,"wa")

    with v as mycsvfile:
        fieldnames = ["gb_id","movie_name","release_year","rovi_id","projectx_id","projectx_videos_link_flag","preprod_videos_link_flag","wrong_sequence_number","credits exists preprod","credits exists projectx","comment","result of sequence number","full_name mismatch","part_name mismatch","Director_name mismatch","writer_name mismatch","Existance of duplicate credits","Duplicate_credits"]

        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        total=0   
        t=0
        for r in range(start,end):
            gb_projectx_id=[]
            rovi_projectx_id=[]
            projectx_credits=[]
            preprod_credits=[]
            total=total+1
            arr_px=[]
            arr_px_duplicate=[]
            arr_preprod=[]
            comment="credits match"
            preprod_videos_link_flag=''
            comment1=''
            projectx_videos_link_flag='' 
            comment2='' 
            px_sequence_number=[]
            pr_sequence_number=[]
            arr_preprod_char_name=[]
            arr_px_char_name=[]
            arr_px_direc_name=[]
            arr_px_writer_name=[]
            comment4=''
            comment3=''
            comment5=''
            comment_=''
            comment__=''
            count=0
            duplicate_credit_comment=''
            arr_gb_credits=[]
            arr_gb_director=[]  
            arr_gb_writer=[] 
            gb_id=str(fullist[r][0])
            movie_name=str(fullist[r][1])
            release_year=str(fullist[r][2])
            rovi_id=str(fullist[r][3])
            try:
                url_rovi="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi" %eval(rovi_id)
                response_rovi=urllib2.Request(url_rovi)
                response_rovi.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_rovi=urllib2.urlopen(response_rovi)
                data_rovi=resp_rovi.read()
                data_resp_rovi=json.loads(data_rovi)
                for ii in data_resp_rovi:
                    if ii.get("type")=='Program' and ii.get("data_source")=='Rovi':
                        rovi_projectx_id.append(ii.get("projectx_id"))
                    else:
                        comment='rovi id not ingested'
 
                if len(rovi_projectx_id)==1:
                    projectx_url="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true&aliases=true&new_images=true"%'{}'.format(",".join([str(i) for i in rovi_projectx_id]))
                    response_px=urllib2.Request(projectx_url)
                    response_px.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_px=urllib2.urlopen(response_px)
                    data_px=resp_px.read()
                    data_resp_px=json.loads(data_px)  
                    
                    preprod_url="https://preprod.caavo.com/programs?ids=%d&ott=true&new_images=true"%eval(rovi_id)
                    response_preprod=urllib2.Request(preprod_url)
                    response_preprod.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                    resp_preprod=urllib2.urlopen(response_preprod)
                    data_preprod=resp_preprod.read()
                    data_resp_preprod=json.loads(data_preprod)
                    for jj in data_resp_px:
                        if jj.get("videos")!=[]:
                            projectx_videos_link_flag='True'
                            if jj.get("credits"):
                                comment__='In projectx credits ingested' 
                                for mm in jj.get("credits"):
                                    if mm.get("credit_type")=='Actor' or mm.get("credit_type")=='Performer' or mm.get("credit_type")=='Voice':
                                        projectx_credits.append({"name":(unidecode.unidecode(mm.get("full_credit_name"))).replace("  "," ")})
                                        projectx_credits.append({"character_name":(unidecode.unidecode(mm.get("part_name"))).replace("  "," ")})
                                        px_sequence_number.append(mm.get("sequence_number"))
                                    if mm.get("credit_type")=='Director':
                                        projectx_credits.append({"director_name":(unidecode.unidecode(mm.get("full_credit_name"))).replace("  "," ")}) 
                                    if mm.get("credit_type")=='Writer':
                                        projectx_credits.append({"writer_name":(unidecode.unidecode(mm.get("full_credit_name"))).replace("  "," ")})  
                            else:
                                comment__='In projectx no credits ingested'
                        else:
                            projectx_videos_link_flag='False'  
                    for kk in data_resp_preprod:
                        if kk.get("videos")!=[]:
                            preprod_videos_link_flag='True'
                            if kk.get("credits"):  
                                comment_='In preprod credits ingested'
                                for mm in kk.get("credits"):
                                    if mm.get("credit_type")=='Actor' or mm.get("credit_type")=='Performer' or mm.get("credit_type")=='Voice':
                                        preprod_credits.append({"name":(unidecode.unidecode(mm.get("full_credit_name"))).replace("  "," ")})
                                        preprod_credits.append({"character_name":(unidecode.unidecode(mm.get("part_name"))).replace("  "," ")})
                                        pr_sequence_number.append(mm.get("sequence_number"))
                                    if mm.get("credit_type")=='Director':
                                        preprod_credits.append({"director_name":(unidecode.unidecode(mm.get("full_credit_name"))).replace("  "," ")})
                                    if mm.get("credit_type")=='Writer':
                                        preprod_credits.append({"writer_name":(unidecode.unidecode(mm.get("full_credit_name"))).replace("  "," ")}) 
                            else:
                                comment_='In preprod no credits ingested'
                        else:
                            preprod_videos_link_flag='False'


                    for k in projectx_credits:
                        if k.keys() == ['name']:
                            count=count + 1

                    b=list(set(px_sequence_number)-set(pr_sequence_number))

                    if b!=[1,2] and b!=[1] and b!=[3] and b!=[4] and b!=[2] and b!=[0,1] and b!=[] and comment_=='In preprod credits ingested' and (count ==1 or count==0):
                        comment6='sequence number wrong'
                    else:
                        comment6='sequence number match' 
                        b=[]
                    if projectx_videos_link_flag=='True' and preprod_videos_link_flag=='True':
                        for aa in projectx_credits:
                            if projectx_credits.count(aa)>1:
                                if preprod_credits==projectx_credits:
                                    if aa.get("name")=='' or aa.get("character_name")=='' or aa.get("character_name")=='Himself' or aa.get("character_name")=='himself' or aa.get("character_name")=='herself' or aa.get("character_name")=='Herself' or aa.get("character_name")=="Himself (Archive Footage)" or aa.get("character_name")=="Herself (Archive Footage)" or aa.get("character_name")=="Self" or aa.get("character_name")=="as himself" or aa.get("character_name")=="as herself" or aa.get("character_name")=="Himself (archive footage)" or aa.get("character_name")=="Herself (archive footage)" or aa.get("character_name")=='Narrator' or aa.get("character_name")=='Reader (Voice)' or aa.get("character_name")=='Team Member' or aa.get("character_name")=='Commentator' or aa.get("character_name")=='Narrator (voice)' or aa.get("character_name")=='Model' or aa.get("character_name")=='Himself (voice)' or aa.get("character_name")=='Reader (Voice)' or aa.get("character_name")=='Himself - The Band' or aa.get("character_name")=='Voice' or aa.get("character_name")=='Interviewpartner' or aa.get("character_name")=='Rider' or aa.get("character_name")=='Racer':
                                        pass
                                    else:
                                        duplicate_credit_comment='duplicate credits exists projectx and preprod'
                                        arr_px_duplicate.append(aa)
                                        comment5=arr_px_duplicate   
                                        break  
                                else:
                                    if aa.get("name")=='' or aa.get("character_name")=='' or aa.get("character_name")=='Himself' or aa.get("character_name")=='Herself' or aa.get("character_name")=='himself' or aa.get("character_name")=='herself' or aa.get("character_name")=="Himself (Archive Footage)" or aa.get("character_name")=="Herself (Archive Footage)" or aa.get("character_name")=="Self" or aa.get("character_name")=="as himself" or aa.get("character_name")=="as herself" or aa.get("character_name")=="Himself (archive footage)" or aa.get("character_name")=="Herself (archive footage)" or aa.get("character_name")=='Narrator' or aa.get("character_name")=='Reader (Voice)' or aa.get("character_name")=='Team Member' or aa.get("character_name")=='Commentator' or aa.get("character_name")=='Narrator (voice)' or aa.get("character_name")=='Model' or aa.get("character_name")=='Himself (voice)' or aa.get("character_name")=='Reader (Voice)' or aa.get("character_name")=='Himself - The Band' or aa.get("character_name")=='Voice' or aa.get("character_name")=='Interviewpartner' or aa.get("character_name")=='Rider' or aa.get("character_name")=='Racer':
                                        pass 
                                    else:
                                        duplicate_credit_comment='duplicate credits exists projectx'
                                        arr_px_duplicate.append(aa)
                                        comment5=arr_px_duplicate
                                        break
                        for credit in projectx_credits:
                            if credit in preprod_credits:
                                projectx_credits.remove(credit)
                                if projectx_credits:
                                    for credit in projectx_credits:
                                        if credit in preprod_credits:
                                            projectx_credits.remove(credit) 
                        if projectx_credits:
                            for credit in projectx_credits:
                                if credit in preprod_credits:
                                    projectx_credits.remove(credit)
                        if projectx_credits==[]:  
                            t=t+1
                            comment="credits match"
                            writer.writerow([gb_id,movie_name,release_year,rovi_id,rovi_projectx_id[0],projectx_videos_link_flag,preprod_videos_link_flag,b,comment_,comment__,comment,comment6,comment1,comment2])
                        else:
                            if projectx_credits:
                                query=mytable.find({"gb_id":eval(gb_id),"show_type":'MO'},{"cast.name":1,"cast.character_name":1,"_id":0,"writers.name":1,"directors.name":1}).sort("updated_at", -1).limit(1)
                                for ll in query:
                                    if ll.get("cast")!=[]:
                                        guidebox_credits=ll.get("cast")
                                        for cc in guidebox_credits:
                                            if cc is not None:
                                                arr_gb_credits.append({"name":unidecode.unidecode(cc.get("name"))})
                                                arr_gb_credits.append({"character_name":unidecode.unidecode(cc.get("character_name"))})
                                    if ll.get("writers")!=[]: 
                                        guidebox_writer=ll.get("writers") 
                                        for aa in guidebox_writer:
                                            if aa is not None:
                                                arr_gb_writer.append({"writer_name":unidecode.unidecode(aa.get("name"))})
                                    if ll.get("directors")!=[]:
                                        guidebox_director=ll.get("directors")
                                        for bb in guidebox_director:
                                            if bb is not None:
                                                arr_gb_director.append({"director_name":unidecode.unidecode(bb.get("name"))})


                                
                                for aa in projectx_credits:
                                    if aa.keys()==["name"]:
                                        query1="SELECT full_credit_name FROM branch_service_rovi.rovi_programs_credits where program_id=%s and full_credit_name=%s;"
                                        cur.execute(query1,(rovi_id,aa.get("name")))
                                        res1=cur.fetchall()
                                        if res1:
                                            projectx_credits.remove(aa)
                                        else:
                                            if arr_gb_credits!=[]:
                                                if aa in arr_gb_credits:
                                                    projectx_credits.remove(aa) 
                                                else:
                                                    arr_px.append(aa)
                                                    comment1={"credits mismatch":  arr_px} 
                                                    comment="credits mismatch"
                            
                                if projectx_credits:
                                    for aa in projectx_credits:
                                        if aa.keys()==["character_name"]:
                                            query2="SELECT full_credit_name FROM branch_service_rovi.rovi_programs_credits where program_id=%s and part_name=%s;"
                                            cur.execute(query2,(rovi_id,aa.get("character_name"),))
                                            res2=cur.fetchall()
                                            if res2:
                                                projectx_credits.remove(aa)
                                            else:
                                                if arr_gb_credits!=[]:
                                                    if aa in arr_gb_credits:
                                                        projectx_credits.remove(aa) 
                                                    else:
                                                        arr_px_char_name.append(aa)
                                                        comment2={"credits mismatch":  arr_px_char_name} 
                                                        comment="credits mismatch"     
                                
                               
                                if projectx_credits:
                                    for aa in projectx_credits: 
                                        if aa.keys()==['director_name']:
                                            query3="SELECT full_credit_name FROM branch_service_rovi.rovi_programs_credits where program_id=%s and full_credit_name=%s;"
                                            cur.execute(query3,(rovi_id,aa.get("director_name"),))
                                            res3=cur.fetchall()
                                            if res3:
                                                projectx_credits.remove(aa)
                                            else:
                                                if arr_gb_director!=[]:
                                                    if aa in arr_gb_director:
                                                        projectx_credits.remove(aa)
                                                    else: 
                                                        arr_px_direc_name.append(aa)
                                                        comment3={"director name mismatch" : arr_px_direc_name}     
                                                        comment="credits mismatch"

                            
                                if projectx_credits:
                                    for aa in projectx_credits:
                                        if aa.keys()==['writer_name']:
                                            query4="SELECT full_credit_name FROM branch_service_rovi.rovi_programs_credits where program_id=%s and full_credit_name=%s;"
                                            cur.execute(query4,(rovi_id,aa.get("writer_name"),))
                                            res4=cur.fetchall()
                                            if res4:
                                                projectx_credits.remove(aa)
                                            else:
                                                if arr_gb_writer!=[]:
                                                    if aa in arr_gb_writer:
                                                        projectx_credits.remove(aa)
                                                    else:
                                                        arr_px_writer_name.append(aa)
                                                        comment4={"writer name mismatch" : arr_px_writer_name}
                                                        comment="credits mismatch"
                             
                                writer.writerow([gb_id,movie_name,release_year,rovi_id,rovi_projectx_id[0],projectx_videos_link_flag,preprod_videos_link_flag,b,comment_,comment__,comment,comment6,comment1,comment2,comment3,comment4,duplicate_credit_comment,comment5])
                            else:     
                                t=t+1    
                                comment='credits match'
                                writer.writerow([gb_id,movie_name,release_year,rovi_id,rovi_projectx_id[0],projectx_videos_link_flag,preprod_videos_link_flag,b,comment_,comment__,comment,comment6,comment1,comment2,comment3,comment4,duplicate_credit_comment,comment5])  
                else:
                    comment="multiple projectx ids found"
                    writer.writerow([gb_id,movie_name,release_year,rovi_id,rovi_projectx_id,projectx_videos_link_flag,preprod_videos_link_flag,comment,comment1,comment2])
                print ({"total":total, "gb_id":gb_id, "rovi_id":rovi_id , "name": name, "credit_match":t})
            except httplib.BadStatusLine:
                print ("exception caught httplib.BadStatusLine................................................................................")
                continue
            except urllib2.HTTPError:
                print ("exception caught HTTPError............................................................................................")
                continue
            except socket.error:
                print ("exception caught SocketError..........................................................................................")
                continue
            except URLError:
                print ("exception caught URLError.............................................................................................")
                continue
    connection.close()
    print datetime.datetime.now() 



t1=threading.Thread(target=credits_checking,args=(1,"starting thread-1",6001,1))
t1.start()
t2=threading.Thread(target=credits_checking,args=(6001,"starting thread-2",12001,2))
t2.start()
t4=threading.Thread(target=credits_checking,args=(12001,"starting thread-4",18001,4))
t4.start()
t3=threading.Thread(target=credits_checking,args=(18001,"starting thread-3",27742,3))
t3.start()

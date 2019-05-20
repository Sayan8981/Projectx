"""Writer: Saayan"""

import MySQLdb
import collections
from pprint import pprint
import sys
import csv
import os
import pymysql
import datetime
import pymongo

def open_csv():
 
    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["ozone"]
    mytable=mydb["guidebox_program_details"]

    inputFile="TVShows_mapped_ROVI_to_GB1"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_mapping_SM_new.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        conn=pymysql.connect(user="projectx",passwd="projectx",host="34.201.242.195",db="projectx",port=3370)
        cur=conn.cursor()
        fieldnames = ["Gb_sm_id","Rovi_sm_id","series_title","projectx_id_rovi","projectx_id_gb","Comment","Result of mapping series"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        j=0
        k=0
        l=0
        m=0
	n=0
        p=0
        total=0
        for r in range(1,len(fullist)):
            total=total+1
            gb_sm_id=str(fullist[r][1])
            rovi_sm_id=str(fullist[r][0])
    
            query1="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='GuideBox' and type='Program' and sub_type='SM'"
            query2="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='Rovi' and type='Program'"
            cur.execute(query1,(gb_sm_id,))
            gb_projectx_id=cur.fetchall()
            print gb_projectx_id
            cur.execute(query2,(rovi_sm_id,))
            rovi_projectx_id=cur.fetchall()
            print rovi_projectx_id
            query3=mytable.find({"gb_id":eval(gb_sm_id),"show_type":'SM'},{"title":1}).limit(1)
            for bb in query3:
                series_title=bb.get("title").encode('ascii','ignore')
           
            if len(rovi_projectx_id)>1 or len(gb_projectx_id)>1:
               
                rovi_projectx_id=list(rovi_projectx_id)
                gb_projectx_id=list(gb_projectx_id)
            
                if gb_projectx_id and rovi_projectx_id:
                    j=j+1
                    a=gb_projectx_id
                    print a
                    b=rovi_projectx_id
                    print b
		    status='Nil'
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Result of mapping series":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Result of mapping series":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})

                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Result of mapping series":'Pass',"Comment":'Multiple ingestion for same content of GB'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Result of mapping series":'Fail',"Comment":'Fail:Multiple ingestion for same content of GB'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Multiple ingestion for same content of both sources',"Result of mapping series":'Pass'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Fail:Multiple ingestion for same content of both sources',"Result of mapping series":'Fail'})

            if len(rovi_projectx_id)==1 and len(gb_projectx_id)==1:
                if rovi_projectx_id == gb_projectx_id:
                    k=k+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Comment":'Pass',"Result of mapping series":'Pass'})

                if rovi_projectx_id != gb_projectx_id:
                    l=l+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Comment":'Fail',"Result of mapping series":'Fail'})

            if gb_projectx_id and not rovi_projectx_id:
                m=m+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":'Nil',"projectx_id_gb":''.join(map(str,[i[:] for i in list(gb_projectx_id)])),"Comment":'No ingestion of rovi_id',"Result of mapping series":'Fail'})
            if rovi_projectx_id and not gb_projectx_id:
                n=n+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":','.join(map(str,[i[:] for i in list(rovi_projectx_id)])),"projectx_id_gb":'Nil',"Comment":'No ingestion of gb_id',"Result of mapping series":'Fail'})
            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Rovi_sm_id":rovi_sm_id,"series_title":series_title,"projectx_id_rovi":'',"projectx_id_gb":'',"Comment":'No Ingestion',"Result of mapping series":'N.A'})
            print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "gb Id not ingested : ",n, "both not ingested: ",p)
    print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "gb Id not ingested : ",n, "both not ingested: ",p)
    print(datetime.datetime.now())


open_csv()


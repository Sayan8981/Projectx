"""Writer: Saayan"""

import MySQLdb
import collections
from pprint import pprint
import sys
import csv
import os
import pymysql
import datetime

def open_csv():
    inputFile="GB_Movie_Rovi"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_mapping_MO_new.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        conn=pymysql.connect(user="projectx",passwd="projectx",host="34.201.242.195",db="projectx",port=3370)
        cur=conn.cursor()
        fieldnames = ["Gb_id","Rovi_id","Movie_name","Release_year","projectx_id_rovi","projectx_id_gb","Comment","Result of mapping"]
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
            gb_id=str(fullist[r][0])
            rovi_id=str(fullist[r][3])
            Movie_name=str(fullist[r][1])
            Release_year=str(fullist[r][2])
            query1="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='GuideBox' and type='Program' and sub_type='MO'"
            query2="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='Rovi' and type='Program'"
            cur.execute(query1,(gb_id,))
            gb_projectx_id=cur.fetchall()
            print gb_projectx_id
            cur.execute(query2,(rovi_id,))
            rovi_projectx_id=cur.fetchall()
            print rovi_projectx_id
            
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
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Result of mapping":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Result of mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})

                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Result of mapping":'Pass',"Comment":'Multiple ingestion for same content of GB'})
                        if status=='Nil':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Result of mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of GB'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b:
				status='Pass'
                                break;
			if status=='Pass':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Multiple ingestion for same content of both sources',"Result of mapping":'Pass'})
                        if status=='Nil':
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Fail:Multiple ingestion for same content of both sources',"Result of mapping":'Fail'})

            if len(rovi_projectx_id)==1 and len(gb_projectx_id)==1:
                if rovi_projectx_id == gb_projectx_id:
                    k=k+1
                    writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Comment":'Pass',"Result of mapping":'Pass'})

                if rovi_projectx_id != gb_projectx_id:
                    l=l+1
                    writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Comment":'Fail',"Result of mapping":'Fail'})

            if gb_projectx_id and not rovi_projectx_id:
                m=m+1
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":'Nil',"projectx_id_gb":''.join(map(str,[i[:] for i in list(gb_projectx_id)])),"Comment":'No ingestion of rovi_id',"Result of mapping":'Fail'})
            if rovi_projectx_id and not gb_projectx_id:
                n=n+1
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":','.join(map(str,[i[:] for i in list(rovi_projectx_id)])),"projectx_id_gb":'Nil',"Comment":'No ingestion of gb_id',"Result of mapping":'Fail'})
            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"Movie_name":Movie_name,"Release_year":Release_year,"projectx_id_rovi":'',"projectx_id_gb":'',"Comment":'No Ingestion',"Result of mapping":'N.A'})
            print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "gb Id not ingested : ",n, "both not ingested: ",p)
    print("total id for both:", total,"multiple mapped: ",j ,"mapped :",k ,"Not mapped :", l, "rovi id not ingested :", m, "gb Id not ingested : ",n, "both not ingested: ",p)
    print(datetime.datetime.now())


open_csv()


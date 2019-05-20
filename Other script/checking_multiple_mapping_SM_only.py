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
    inputFile="Book3_SM"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_Series.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        conn=pymysql.connect(user="projectx",passwd="projectx",host="52.91.200.186",db="projectx",port=3370)
        cur=conn.cursor()
        fieldnames = ["Gb_sm_id","projectx_id_gb","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        j=0
        k=0
        l=0
        m=0
        n=0
        p=0
        q=0
        r=0
        total=0

        for r in range(1,len(fullist)):
            total=total+1
            gb_sm_id=int(fullist[r][0])
            print gb_sm_id
          
            query1="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='Guidebox' and type='Program' and sub_type='SM'"
            cur.execute(query1,(gb_sm_id,))
            gb_projectx_id=cur.fetchall()
            print gb_projectx_id

#            print("total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "gb Id not ingested : ",n+r, "both not ingested: ",p)

	    if len(gb_projectx_id)>1:
                
                gb_projectx_id=list(gb_projectx_id)
                writer.writerow({"Gb_sm_id":gb_sm_id,"projectx_id_gb":''.join(map(str,[i[:] for i in list(gb_projectx_id)])),"Comment":'Multiple mapped ids'})
 
            if len(gb_projectx_id)==0 :
                p=p+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"projectx_id_gb":'',"Comment":'No Ingestion of SM'})



    print(datetime.datetime.now())
open_csv()


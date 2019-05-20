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

    inputFile="not ingestion_aprx_15000"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/NO_ingestion_GB.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        conn=pymysql.connect(user="projectx",passwd="projectx",host="18.232.135.33",db="projectx",port=3370)
        cur=conn.cursor()

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
        r=0
        total=0

        for r in range(1,len(fullist)):
            total=total+1
            gb_id=str(fullist[r][1])
            rovi_id=str(fullist[r][2])	
            print gb_id
            print rovi_id
            gb_sm_id=int(fullist[r][0])
            print gb_sm_id
            episode_title=str(fullist[r][3])
	    ozoneepisodetitle=str(fullist[r][4])
	    OzoneOriginalEpisodeTitle=str(fullist[r][5]) 
            print("total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "gb Id not ingested : ",n+r, "both not ingested: ",p) 

            query1="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='GuideBox' and type='Program' and sub_type='SE'"
            query2="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='Rovi' and type='Program'"
            cur.execute(query1,(gb_id,))
            gb_projectx_id=cur.fetchall()
            print gb_projectx_id
            cur.execute(query2,(rovi_id,))
            rovi_projectx_id=cur.fetchall()
            print rovi_projectx_id
             

            if len(rovi_projectx_id)==1 and len(gb_projectx_id)==1:
                if rovi_projectx_id == gb_projectx_id:
                    j=j+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Episode mapping":'Pass',"Comment":'Pass'})
                if rovi_projectx_id != gb_projectx_id:
                    k=k+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Episode mapping":'Fail',"Comment":'Fail'})
            
	    if len(rovi_projectx_id)>1 or len(gb_projectx_id)>1:
                
                rovi_projectx_id=list(rovi_projectx_id)
                gb_projectx_id=list(gb_projectx_id)

                if gb_projectx_id and rovi_projectx_id:
                    l+l+1
                    a=gb_projectx_id
                    print a
                    b=rovi_projectx_id
                    print b
#                    import pdb;pdb.set_trace()
                    status='Nil'
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Episode mapping":'Pass',"Comment":'Multiple ingestion for same content of rovi'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Episode mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of rovi'})
                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
                                status='Pass'
                                break;
                        if status=='Pass':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Episode mapping":'Pass',"Comment":'Multiple ingestion for same content of GB'})
                        if status=='Nil':
                            writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Episode mapping":'Fail',"Comment":'Fail:Multiple ingestion for same content of GB'})

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
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'Nil',"projectx_id_gb":''.join(map(str,[i[:] for i in list(gb_projectx_id)])),"Episode mapping":'N.A',"Comment":'No ingestion of rovi_id of episodes and multiple ingestion for GB_id'})

                if rovi_projectx_id and not gb_projectx_id:
                    n=n+1
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":','.join(map(str,[i[:] for i in list(rovi_projectx_id)])),"projectx_id_gb":'Nil',"Episode mapping":'N.A',"Comment":'No ingestion of gb_id and multiple ingestion for Rovi_id'})

            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==0:
                p=p+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'',"projectx_id_gb":'',"Episode mapping":'N.A',"Comment":'No Ingestion of both sources'})

            if len(gb_projectx_id)==1 and len(rovi_projectx_id)==0:
                q=q+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'',"projectx_id_gb":str((gb_projectx_id[0])[0]),"Episode mapping":'Fail',"Comment":'No Ingestion of rovi_id of episode'})
            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==1:
                r=r+1
                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":'',"Episode mapping":'Fail',"Comment":'No Ingestion of gb_id of episode'})


    print("total id for both:", total,"multiple mapped: ",l,"mapped :",j ,"Not mapped :", k, "rovi id not ingested :", m+q, "gb Id not ingested : ",n+r, "both not ingested: ",p)
    print(datetime.datetime.now())
open_csv()


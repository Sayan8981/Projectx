"""Writer: Saayan"""

import MySQLdb
import collections
from pprint import pprint
import sys
import csv
import os

def open_csv():
    import pdb;pdb.set_trace()
    inputFile="GB_TvShow_Rovi"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_cases_Series.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["Gb_sm_id","Gb_id","Rovi_id","projectx_id_rovi","projectx_id_gb","Episode mapping","mapping of series","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()

        for r in range(1,len(fullist)):
            gb_id=str(fullist[r][4])
            rovi_id=str(fullist[r][10])
            print gb_id
            print rovi_id
            gb_sm_id=int(fullist[r][0])
            print gb_sm_id
     
            conn=MySQLdb.connect(user="root",passwd="branch@123",host="localhost",db="projectx")
            cur=conn.cursor()
          
            query1="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='Guidebox' and type='Program' and sub_type='SE'"
            query2="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='Rovi' and type='Program'"
            cur.execute(query1,(gb_id,))
            gb_projectx_id=cur.fetchall()
            print gb_projectx_id
            cur.execute(query2,(rovi_id,))
            rovi_projectx_id=cur.fetchall()
            print rovi_projectx_id
            #import pdb;pdb.set_trace()
            query3="select series_id from projectx.Programs where id=%s and show_type='SE'"
            query4="select series_id from projectx.Programs where id=%s and show_type='SE'"
            if len(rovi_projectx_id)==1 and len(gb_projectx_id)==1:
                if rovi_projectx_id == gb_projectx_id:
                    cur.execute(query3,((rovi_projectx_id[0])[0],))
                    res1=cur.fetchall()
                    res1=str((res1[0])[0])
                    cur.execute(query4,((gb_projectx_id[0])[0],))
                    res2=cur.fetchall()
                    res2=str((res2[0])[0]) 
                    if int((res2[0])[0])==int((res1[0])[0]):
                        writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Episode mapping":'episode mapping Pass',"mapping of series":'Pass'+str((res2[0])[0]),"Comment":'Pass'})
                    else:
                        writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Episode mapping":'episode mapping Pass',"mapping of series":'Fail' +str((res2[0])[0])+','+str((res1[0])[0]),"Comment":'Fail: Series wrongly mapped'})

                if rovi_projectx_id != gb_projectx_id:
                    cur.execute(query4,((gb_projectx_id[0])[0],))
                    res3=cur.fetchall()
                    res3=str((res3[0])[0])
                    cur.execute(query4,((rovi_projectx_id[0])[0],))
                    res4=cur.fetchall()
                    res4=str((res4[0])[0])

                    if int((res4[0])[0])==int((res4[0])[0]):
                        writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Episode mapping":'episode mapping Fail',"mapping of series":'Pass'+ str((res4[0])[0]),"Comment":'Fail'})
                    else:
                        writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Episode mapping":'episode mapping Fail',"mapping of series":'Fail'+ str((res4[0])[0])+ int((res3[0])[0]),"Comment":'Fail'})  
            if len(rovi_projectx_id)>1 or len(gb_projectx_id)>1:
                rovi_projectx_id=list(rovi_projectx_id)
                gb_projectx_id=list(gb_projectx_id)
                rovi_projectx_id=list(map(str, [i[:] for i in rovi_projectx_id]))
                gb_projectx_id=list(map(str, [i[:] for i in gb_projectx_id]))

                if gb_projectx_id and rovi_projectx_id:
                    a=gb_projectx_id
                    print a
                    b=rovi_projectx_id
                    print b
                    if len(a)==1 and len(b)>1:
                        for x in a:
                            if x in b:
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Episode mapping":'Pass',"mapping of series":'N.A',"Comment":'Fail:Multiple ingestion for same content of rovi'})
                            else:
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Episode mapping":'Fail',"mapping of series":'N.A',"Comment":'Fail:Multiple ingestion for same content of rovi'})

                    if len(a)>1 and len(b)==1:
                        for x in b:
                            if x in a:
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Episode mapping":'Pass',"mapping of series":'N.A',"Comment":'Fail:Multiple ingestion for same content of GB'})
                            else:
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Episode mapping":'Fail',"mapping of series":'N.A',"Comment":'Fail:Multiple ingestion for same content of GB'})

                    if len(a)>1 and len(b)>1:
                        for x in a:
                            if x in b: 
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Comment":'Fail:Multiple ingestion for same content of both sources',"Episode mapping":'Pass',"mapping of series":'N.A'})
                            else:
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":','.join(map(str,[i[:] for i in rovi_projectx_id])),"projectx_id_gb":','.join(map(str,[i[:] for i in gb_projectx_id])),"Comment":'Fail:Multiple ingestion for same content of both sources',"Episode mapping":'Fail',"mapping of series":'N.A'})

                if gb_projectx_id and not rovi_projectx_id:
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":'Nil',"projectx_id_gb":''.join(map(str,[i[:] for i in list(gb_projectx_id)])),"Episode mapping":'N.A',"Comment":'No ingestion of rovi_id of episodes and multiple ingestion for GB_id',"mapping of series":'N.A'})

                if rovi_projectx_id and not gb_projectx_id:
                    writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":','.join(map(str,[i[:] for i in list(rovi_projectx_id)])),"projectx_id_gb":'Nil',"Episode mapping":'N.A',"Comment":'No ingestion of gb_id and multiple ingestion for Rovi_id',"mapping of series":'N.A'})

            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==0:
                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_gb":'',"Episode mapping":'N.A',"Comment":'No Ingestion of both sources',"mapping of series":'N.A'})

            if len(gb_projectx_id)==1 and len(rovi_projectx_id)==0:
                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_gb":str((gb_projectx_id[0])[0]),"Episode mapping":'Fail',"Comment":'No Ingestion of rovi_id of episode',"mapping of series":'N.A'})
            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==1:
                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":'',"Episode mapping":'Fail',"Comment":'No Ingestion of gb_id of episode',"mapping of series":'N.A'})



open_csv()

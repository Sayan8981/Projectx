"""Writer: Saayan"""

import MySQLdb
import collections
from pprint import pprint
import sys
import csv
import os

def open_csv():
    inputFile="GB_Movie_Rovi"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/Result_cases_MO.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["Gb_id","Rovi_id","projectx_id_rovi","projectx_id_gb","Comment","show_type","variant_parent_id","variant_id_rovi","Exceptionlogs error message"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()

        for r in range(1,len(fullist)):
            gb_id=str(fullist[r][0])
            rovi_id=str(fullist[r][3])
    
            conn=MySQLdb.connect(user="root",passwd="branch@123",host="localhost",db="projectx")
            cur=conn.cursor()
            query1="SELECT projectx_id FROM projectx.ProjectxMaps where source_id=%s and data_source='Guidebox' and type='Program' and sub_type='MO'"
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
                rovi_projectx_id=','.join(map(str, [i[:] for i in rovi_projectx_id]))
                gb_projectx_id=','.join(map(str, [i[:] for i in gb_projectx_id]))
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_gb":gb_projectx_id,"Comment":'Fail:Multiple ingestion for same content',"show_type":'',"variant_parent_id":'Nil',"variant_rovi":'Nil',"Exceptionlogs error message":'N.A'})
            
            if len(rovi_projectx_id)==1 and len(gb_projectx_id)==1:
                if rovi_projectx_id == gb_projectx_id:
                    writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Comment":'Pass',"show_type":'MO',"variant_parent_id":'Nil',"variant_id_rovi":'Nil',"Exceptionlogs error message":'N.A'})

                if rovi_projectx_id != gb_projectx_id:
                    query3="select show_type from projectx.Programs where id=%s"
                    cur.execute(query3,(str((rovi_projectx_id[0])[0]),))
                    res3=cur.fetchall()
                    print res3
                    if str((res3[0])[0])=='OT':
                        writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Comment":'Fail',"show_type":str((res3[0])[0])+ 'type of rovi_id',"variant_parent_id":'Nil',"variant_id_rovi":'Nil',"Exceptionlogs error message":'N.A'})

                    elif str((res3[0])[0])=='MO':
                        query4="select variant_parent_id from projectx.Programs where id=%s and show_type='MO'"
                        cur.execute(query4,(str((rovi_projectx_id[0])[0]),))
                        res4=cur.fetchall()
                        print res4
                        if res4[0]:
                            query5="SELECT source_id FROM projectx.ProjectxMaps where projectx_id=%s and data_source='Rovi' and type='Program'";
                            cur.execute(query5,(str((res4[0])[0]),))
                            res5=cur.fetchall()
                            print res5
                            if res5:
                                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Comment":'Fail',"show_type":str((res3[0])[0]),"variant_parent_id":str((res4[0])[0]),"variant_id_rovi":(res5[0])[0],"Exceptionlogs error message":'N.A'})
                        else:
                            writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":str((rovi_projectx_id[0])[0]),"projectx_id_gb":str((gb_projectx_id[0])[0]),"Comment":'Fail',"show_type":str((res3[0])[0]),"variant_parent_id":'Nil',"variant_id_rovi":'Nil',"Exceptionlogs error message":'N.A'})

            query6="SELECT error_message FROM projectx.ExceptionLogs where redis_key=%s"
            query7="SELECT error_message FROM projectx.ExceptionLogs where redis_key=%s"
            if gb_projectx_id and not rovi_projectx_id:
                rovi_idd=str('rovi-'+str((rovi_id)))
                cur.execute(query7,(rovi_idd,))
                res7=cur.fetchall()
                res7=str(res7)
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":'Nil',"projectx_id_gb":''.join(map(str,[i[:] for i in list(gb_projectx_id)])),"Comment":'No ingestion of rovi_id',"show_type":'',"variant_parent_id":'Nil',"variant_id_rovi":'Nil',"Exceptionlogs error message":res7})
            if rovi_projectx_id and not gb_projectx_id:
                gb_idd=str('gb-'+str(gb_id)+'-MO')
                cur.execute(query6,(gb_idd,))
                res6=cur.fetchall()
                res6=str(res6)
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":','.join(map(str,[i[:] for i in list(rovi_projectx_id)])),"projectx_id_gb":'Nil',"Comment":'No ingestion of gb_id',"show_type":'',"variant_parent_id":'Nil',"variant_id_rovi":'Nil',"Exceptionlogs error message":res6})
            if len(gb_projectx_id)==0 and len(rovi_projectx_id)==0:
                writer.writerow({"Gb_id":gb_id,"Rovi_id":rovi_id,"projectx_id_rovi":'',"projectx_id_gb":'',"Comment":'No Ingestion',"show_type":'',"variant_parent_id":'Nil',"variant_id_rovi":'Nil',"Exceptionlogs error message":'N.A'})


open_csv()

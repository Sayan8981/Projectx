"""Writer: Saayan"""

import threading
import csv
import pymysql
import datetime
import sys
import urllib2
import json
import os
from urllib2 import URLError,HTTPError
import httplib
import socket
import pinyin
import unidecode
sys.path.insert(0,os.getcwd()+'/common_lib')
sys.path.insert(3,os.getcwd()+'/common_lib/validation_lib')
import initialization_file
import checking_only_mapped_px_id
import getting_meta
import ott_validation 


"""
source_details={"source_link_present":hbogo_link_present,"source_id":hbogo_id,"show_type":show_type}
projectx_details={"px_video_link_present":px_video_link_present,"launch_id":launch_id}

only_mapped_ids={"px_id":px_id[0],"source_id":_id,"source_flag":source_flag,"source_map":source_map}
"""

#TODO: To check OTT links
def ott_checking(only_mapped_ids,hbogo_id,show_type,thread_name,source,writer,hbogo_data,aa):
    #import pdb;pdb.set_trace()
    retry_count=0
    try:     
        if only_mapped_ids["source_flag"]=='True':
            initialization_file.total+=1
            #import pdb;pdb.set_trace()
            projectx_id=only_mapped_ids["px_id"]
            source_id=only_mapped_ids["source_id"]
            print("\n")
            print ({"total_only_hbogo_mapped":initialization_file.total,"%s_id"%source:hbogo_id,"Px_id":projectx_id,"%s_id"%source:source_id,"thread_name":thread_name,"source_map":only_mapped_ids["source_map"]})
            source_details=getting_meta.getting_source_details(source_id,show_type,source,hbogo_data,aa)
            projectx_details=getting_meta.getting_projectx_details(projectx_id,show_type,source)
            if projectx_details!='Null':
                if hbogo_data.get("purchase_types")!=[]:
                    ott_validation_result=ott_validation.ott_validation(projectx_details,source_id)
                    writer.writerow([source_id,projectx_id,show_type,projectx_details["px_video_link_present"],source_details["source_link_present"],ott_validation_result,only_mapped_ids["source_flag"],only_mapped_ids["source_map"]])
                else:
                    writer.writerow([source_id,projectx_id,show_type,'','','',only_mapped_ids["source_flag"]])     
            else:
                writer.writerow([source_id,projectx_id,show_type,projectx_details["px_video_link_present"],source_details["source_link_present"],only_mapped_ids["source_flag"],'Px_response_null',only_mapped_ids["source_map"]])
        elif only_mapped_ids["source_flag"]=='True(Rovi+others)':
            if show_type!='SM':
                projectx_id=only_mapped_ids["px_id"]
                source_id=only_mapped_ids["source_id"]
                print("\n")
                print ({"Px_id":projectx_id,"%s_id"%source:source_id,"thread_name":thread_name})

                projectx_details=getting_meta.getting_projectx_details(projectx_id,show_type,source)
                if projectx_details!='Null':
                    if hbogo_data.get("purchase_types")!=[]:
                        ott_validation_result=ott_validation.ott_validation(projectx_details,source_id)
                        writer.writerow([source_id,projectx_id,show_type,'','',ott_validation_result,only_mapped_ids["source_flag"],only_mapped_ids["source_map"]])
                else:
                    writer.writerow([source_id,projectx_id,show_type,'','','',only_mapped_ids["source_flag"]])                                                                
        else:            
            writer.writerow([hbogo_id,'',show_type,'','',only_mapped_ids,'Px_id_null',only_mapped_ids["source_map"]])          
            
    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
        retry_count+=1
        print("Retrying...................................",retry_count)
        print("\n")
        print ("exception/error caught in ott_checking func.........................",type(e),hbogo_id,show_type,thread_name)
        if retry_count<=5:
            only_mapped_ids=checking_only_mapped_px_id.getting_mapped_px_id(hbogo_id,show_type,source)
            ott_checking(only_mapped_ids,hbogo_id,show_type,thread_name,source,writer,hbogo_data,)  
        else:
            retry_count=0

def mysql_connection_query():
    retry_count=0
    connection=pymysql.connect(user="root",passwd="branch@123",host="192.168.86.10",db="branch_service",port=3306)
    cur=connection.cursor()
    try:
        query="select launch_id,show_type,title,series_title,series_launch_id,url,expired,expired_at,updated_at from hbogo_programs;"
        cur.execute(query)
        hbogo_data=cur.fetchall()
        return hbogo_data
    except (Exception,socket.error) as e:
        retry_count+=1
        if retry_count<=5:
            mysql_connection_query()        
    connection.close()            

#TODO: to open file to print O/P
def create_csv(result_sheet):
    #import pdb;pdb.set_trace()
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    output_file=open(os.getcwd()+result_sheet,"wa")
    return output_file

#TODO:main func
def main(start_id,thread_name,end_id,page_id,source):
    result_sheet='/result/hbogo_ott_checking%d.csv'%page_id
    output_file=create_csv(result_sheet)
    with output_file as mycsvfile:
        fieldnames = ["%s_id"%source,"Projectx_id","show_type","px_video_link_present","%s_link_present"%source,"ott_link_result","mapping"]
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        hbogo_data=mysql_connection_query()
        for aa in range(start_id,end_id):
            print({"skip":aa})
            if (hbogo_data[aa])[1] is not None:
                #import pdb;pdb.set_trace()
                hbogo_id=(hbogo_data[aa])[0]
                show_type=(hbogo_data[aa])[1]
                initialization_file.count_hbogo_id+=1
                print("\n")
                print ({"count_hbogo_id":initialization_file.count_hbogo_id,"thread_name":thread_name})
                only_mapped_ids=checking_only_mapped_px_id.getting_mapped_px_id(hbogo_id,show_type,source)
                ott_checking(only_mapped_ids,hbogo_id,show_type,thread_name,source,writer,hbogo_data,aa)              
        print("\n")                    
        print ({"count_hbogo_id":count_hbogo_id,"name":thread_name})  
    output_file.close()                      


#TODO: create threading
def threading_pool():    

    t1=threading.Thread(target=main,args=(1,"thread-1",2000,1,initialization_file.source))
    t1.start()
    t2=threading.Thread(target=main,args=(2000,"thread-2",4000,2,initialization_file.source))
    t2.start()
    t3=threading.Thread(target=main,args=(4000,"thread-3",6000,3,initialization_file.source))
    t3.start()
    t4=threading.Thread(target=main,args=(6000,"thread-4",6884,4,initialization_file.source))
    t4.start()

#Starting    
threading_pool()



#total 165204
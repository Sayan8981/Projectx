"""Writer: Saayan"""

import threading
import csv
import pymongo
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
source_details={"source_link_present":vudu_link_present,"source_id":vudu_id,"show_type":show_type}
projectx_details={"px_video_link_present":px_video_link_present,"launch_id":launch_id}

only_mapped_ids={"px_id":px_id[0],"source_id":_id,"source_flag":source_flag,"source_map":source_map}
"""

#TODO: To check OTT links
def ott_checking(only_mapped_ids,vudu_id,show_type,thread_name,source,writer,data,vudu_table):
    #import pdb;pdb.set_trace()
    retry_count=0
    try:     
        if only_mapped_ids["source_flag"]=='True':
            initialization_file.total+=1
            #import pdb;pdb.set_trace()
            projectx_id=only_mapped_ids["px_id"]
            source_id=only_mapped_ids["source_id"]
            print("\n")
            print ({"total_only_vudu_mapped":initialization_file.total,"%s_id"%source:vudu_id,"Px_id":projectx_id,"%s_id"%source:source_id,"thread_name":thread_name,"source_map":only_mapped_ids["source_map"]})
            source_details=getting_meta.getting_source_details(source_id,show_type,source,data,vudu_table)
            projectx_details=getting_meta.getting_projectx_details(projectx_id,show_type,source)
            if projectx_details!='Null':
                if data.get("purchase_types")!=[]:
                    ott_validation_result=ott_validation.ott_validation(projectx_details,source_id)
                    writer.writerow([source_id,projectx_id,show_type,projectx_details["px_video_link_present"],source_details["source_link_present"],ott_validation_result,only_mapped_ids["source_flag"],only_mapped_ids["source_map"]])
                else:
                    writer.writerow([source_id,projectx_id,show_type,'','','',only_mapped_ids["source_flag"],'purchase_type_null'])     
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
                    if data.get("purchase_types")!=[]:
                        ott_validation_result=ott_validation.ott_validation(projectx_details,source_id)
                        writer.writerow([source_id,projectx_id,show_type,'','',ott_validation_result,only_mapped_ids["source_flag"],only_mapped_ids["source_map"]])
                else:
                    writer.writerow([source_id,projectx_id,show_type,'','','',only_mapped_ids["source_flag"],'purchase_type_null'])                                                                
        else:            
            writer.writerow([vudu_id,'',show_type,'','',only_mapped_ids,'Px_id_null',only_mapped_ids["source_map"]])          
            
    except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError,RuntimeError) as e:
        retry_count+=1
        print("Retrying...................................",retry_count)
        print("\n")
        print ("exception/error caught in ott_checking func.........................",type(e),vudu_id,show_type,thread_name)
        if retry_count<=5:
            only_mapped_ids=checking_only_mapped_px_id.getting_mapped_px_id(vudu_id,show_type,source)
            ott_checking(only_mapped_ids,vudu_id,show_type,thread_name,source,writer,data,vudu_table)  
        else:
            retry_count=0

#TODO: to open file to print O/P
def create_csv(result_sheet):
    #import pdb;pdb.set_trace()
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    output_file=open(os.getcwd()+result_sheet,"wa")
    return output_file

#TODO:main func
def main(start_id,thread_name,end_id,page_id,vudu_table,source):
    result_sheet='/result/vudu_ott_checking%d.csv'%page_id
    output_file=create_csv(result_sheet)
    with output_file as mycsvfile:
        fieldnames = ["%s_id"%source,"Projectx_id","show_type","px_video_link_present","%s_link_present"%source,"ott_link_result","mapping"]
        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        for aa in range(start_id,end_id,100):
            print({"skip":aa})
            query_vudu=vudu_table.aggregate([{"$match":{"$and":[{"show_type":{"$in":["MO","SE"]}},{"language" : "English"}]}},{"$project":{"launch_id":1,"_id":0,"show_type":1,"series_id":1,"url":1,"purchase_types":1}},{"$skip":aa},{"$limit":100}])  
            for data in query_vudu:
                if data.get("launch_id")!="":
                    #import pdb;pdb.set_trace()
                    vudu_id=data.get("launch_id")
                    show_type=data.get("show_type")
                    initialization_file.count_vudu_id+=1
                    print("\n")
                    print ({"count_vudu_id":initialization_file.count_vudu_id,"thread_name":thread_name})
                    only_mapped_ids=checking_only_mapped_px_id.getting_mapped_px_id(vudu_id,show_type,source)
                    ott_checking(only_mapped_ids,vudu_id,show_type,thread_name,source,writer,data,vudu_table)              
        print("\n")                    
        print ({"count_vudu_id":count_vudu_id,"name":thread_name})  
    output_file.close()                      


#TODO: create threading
def threading_pool():    

    t1=threading.Thread(target=main,args=(0,"thread-1",20000,1,initialization_file.vudu_table,initialization_file.source))
    t1.start()
    t2=threading.Thread(target=main,args=(20000,"thread-2",40000,2,initialization_file.vudu_table,initialization_file.source))
    t2.start()
    t3=threading.Thread(target=main,args=(40000,"thread-3",60000,3,initialization_file.vudu_table,initialization_file.source))
    t3.start()
    t4=threading.Thread(target=main,args=(60000,"thread-4",80000,4,initialization_file.vudu_table,initialization_file.source))
    t4.start()
    t5=threading.Thread(target=main,args=(80000,"thread-5",100000,5,initialization_file.vudu_table,initialization_file.source))
    t5.start()
    t6=threading.Thread(target=main,args=(100000,"thread-6",120000,6,initialization_file.vudu_table,initialization_file.source))
    t6.start()
    t7=threading.Thread(target=main,args=(120000,"thread-7",140000,7,initialization_file.vudu_table,initialization_file.source))
    t7.start()
    t8=threading.Thread(target=main,args=(140000,"thread-8",160000,8,initialization_file.vudu_table,initialization_file.source))
    t8.start()
    t9=threading.Thread(target=main,args=(160000,"thread-9",180000,9,initialization_file.vudu_table,initialization_file.source))
    t9.start()
    t10=threading.Thread(target=main,args=(180000,"thread-10",200000,10,initialization_file.vudu_table,initialization_file.source))
    t10.start()
    t11=threading.Thread(target=main,args=(200000,"thread-11",230000,11,initialization_file.vudu_table,initialization_file.source))
    t11.start()

    initialization_file.connection.close()

#Starting    
threading_pool()



#total 165204
"""Writer: Saayan"""

from multiprocessing import Process
import csv
import pymongo
import MySQLdb
import datetime
import sys
import json
import os
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500)

class gb_ids:

    def __init__(self):
        self.source="GuideBox"
        self.total=0
        self.count_GuideBox_id=0
        self.writer=''

    def mongo_mysql_connection(self):
        self.connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
        self.sourceDB=self.connection["ozone"] 
        self.sourcetable=self.sourceDB["guidebox_program_details"]

    #TODO: Getting_projectx_ids which is only mapped to GuideBox
    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.mongo_mysql_connection()
        result_sheet='/output_gb/%s_id_%s_%s.csv'%(self.source,thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            fieldnames = ["%s_series_id"%self.source,"%s_id"%self.source,"show_type"]
            self.writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
            self.writer.writerow(fieldnames)
            #import pdb;pdb.set_trace()
            for aa in range(start_id,end_id,100):
                try:
                    query_GuideBox=self.sourcetable.aggregate([{"$match":{"$and":[{"show_type":{"$in":
                        ["MO","SE","SM"]}}]}},{"$project":{"gb_id":1,"_id":0,"show_type":1,"show_id":1,
                        "title":1,"release_year":1,"episode_number":1,"season_number":1,"alternate_titles":1,
                        "runtime":1}},{"$skip":aa},{"$limit":100}])
                    for data in query_GuideBox:
                        self.gb_id_sm=''
                        if data.get("gb_id")!="":
                            #import pdb;pdb.set_trace()
                            GuideBox_id=data.get("gb_id")
                            show_type=data.get("show_type")
                            if show_type=='SE':
                                self.gb_id_sm=data.get("show_id")
                            self.count_GuideBox_id+=1
                            print("\n")
                            print("%s_id:"%self.source,GuideBox_id,"thread_name:",thread_name,"count_GuideBox_id:"+str(self.count_GuideBox_id))
                            self.writer.writerow([self.gb_id_sm,GuideBox_id,show_type])
                except (Exception,pymongo.errors.CursorNotFound,socket.error) as e:
                    print("Exception caught in main func................",type(e),thread_name)
                    continue                

        output_file.close()
        self.connection.close()                    

    #TODO: to set up threading part
    def threading_pool(self):    

        t1=Process(target=self.main,args=(0,"thread-1",50000))
        t1.start()
        t2=Process(target=self.main,args=(50030,"thread-2",100000))
        t2.start()                          
        t3=Process(target=self.main,args=(100000,"thread-3",150000))
        t3.start()
        t4=Process(target=self.main,args=(150000,"thread-4",200000))
        t4.start()
        t5=Process(target=self.main,args=(200000,"thread-5",250000))
        t5.start()
        t6=Process(target=self.main,args=(250000,"thread-6",300000))
        t6.start()
        t7=Process(target=self.main,args=(300000,"thread-7",350000))
        t7.start()
        t8=Process(target=self.main,args=(350000,"thread-8",400000))
        t8.start()
        t9=Process(target=self.main,args=(400000,"thread-9",450000))
        t9.start()
        t10=Process(target=self.main,args=(450000,"thread-10",500000))
        t10.start()                          
        t11=Process(target=self.main,args=(500000,"thread-11",550000))
        t11.start()
        t12=Process(target=self.main,args=(550000,"thread-12",600000))
        t12.start()
        t13=Process(target=self.main,args=(600000,"thread-13",650000))
        t13.start()
        t14=Process(target=self.main,args=(650000,"thread-14",700000))
        t14.start()
        t15=Process(target=self.main,args=(700000,"thread-15",750000))
        t15.start()
        t16=Process(target=self.main,args=(750000,"thread-16",800000))
        t16.start()
        t17=Process(target=self.main,args=(800000,"thread-17",850000))
        t17.start()
        t18=Process(target=self.main,args=(850000,"thread-18",900000))
        t18.start()

    #starting     
gb_ids().threading_pool()



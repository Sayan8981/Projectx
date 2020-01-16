
import os
from multiprocessing import Process
import sys
import threading
import datetime
import csv
import json
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules,checking_any_two_px_programs
sys.setrecursionlimit(1500)
sys.path.insert(1,os.getcwd())

class movies_mapp_fail_cases:

    def __init__(self):
        self.fieldnames= ["rovi_id","gb_id","px_id1","px_id1_show_type","px_id1_variant_parent_id","px_id1_is_group_language_primary",
                         "px_id1_record_language","px_id2","px_id2_show_type","px_id2_variant_parent_id","px_id2_is_group_language_primary",
                         "px_id2_record_language","px_id1_credits_null","px_id1_db_credit_present","px_id2_credits_null","px_id2_db_credit_present"
                         ,"long_title_match","original_title_match","runtime_match","release_year_match","alias_title_match","credit_match",
                          "match_link_id","link_match","comment"]                 
        self.px_array=[]
        self.rovi_id=''
        self.gb_id=''
        self.px_id1=''
        self.px_id2=''
        self.total=0

    def cleanup(self):
        self.px_array=[]
        self.rovi_id=''
        self.gb_id=''
        self.px_id1=''
        self.px_id2=''

    def default_param(self):
        self.source="GuideBox"
        self.projectx_domain_name="https://testprojectx.caavo.com"
        self.prod_domain="api.caavo.com"
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        self.projectx_api='https://testprojectx.caavo.com/programs?ids=%s&ott=true&aliases=true'
        self.projectx_mapping_api="http://18.214.4.22:81/projectx/%d/mapping/"
        self.credit_db_api="http://18.214.4.22:81/projectx/%d/credits/"                         

    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.default_param()
        inputFile="input_series_mappFail/guidebox_series_mapp_fail_input"
        input_data=lib_common_modules().read_csv(inputFile)

        result_sheet='/output_automated_fail_cases_series/result_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        
        with output_file as outputcsvfile:
            self.writer= csv.DictWriter(outputcsvfile,fieldnames=self.fieldnames,dialect="excel",
                                                                                 lineterminator = '\n')
            self.writer.writeheader()

            for data in range(start_id,end_id):
                print("Checking same program of map fail series.............")
                self.cleanup()
                self.rovi_id=eval(input_data[data][0])
                self.gb_id=eval(input_data[data][1])
                self.px_id1=eval(input_data[data][2])
                self.px_id2=eval(input_data[data][3])
                #import pdb;pdb.set_trace()
                self.px_array.insert(0,self.px_id1)
                self.px_array.insert(1,self.px_id2)
                print("\n")
                self.total +=1
                print(self.rovi_id,self.gb_id,[self.px_id1,self.px_id2],thread_name,"total:",self.total)
                result=checking_any_two_px_programs().checking_same_program(self.px_array,
                                   self.projectx_api,self.credit_db_api,self.source,self.token)
                result.update({"rovi_id":self.rovi_id,"gb_id":self.gb_id})
                self.writer.writerow(result)
                print("\n")
                print(datetime.datetime.now())


    def thread_pool(self):
        t1=Process(target=self.main,args=(1,"thread-1",100))
        t1.start()
        t2=Process(target=self.main,args=(100,"thread-2",200))
        t2.start()
        t3=Process(target=self.main,args=(200,"thread-3",300))
        t3.start()
        t4=Process(target=self.main,args=(300,"thread-4",500))
        t4.start()
        t5=Process(target=self.main,args=(500,"thread-5",600))
        t5.start()
        t6=Process(target=self.main,args=(600,"thread-6",700))
        t6.start()
        t7=Process(target=self.main,args=(700,"thread-7",900))
        t7.start()
        t8=Process(target=self.main,args=(900,"thread-8",1200))
        t8.start()
        t9=Process(target=self.main,args=(1200,"thread-9",1400))
        t9.start()
        t10=Process(target=self.main,args=(1400,"thread-10",1500))
        t10.start()
        t11=Process(target=self.main,args=(1500,"thread-11",1681))
        t11.start()


movies_mapp_fail_cases().thread_pool()

            

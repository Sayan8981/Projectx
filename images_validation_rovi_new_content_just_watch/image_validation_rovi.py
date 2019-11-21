from multiprocessing import Process
import threading
import sys
import MySQLdb
import os
import csv
import datetime
homedir=os.path.expanduser("~")
sys.path.insert(0,'%s/common_lib'%homedir)
from lib import lib_common_modules
sys.setrecursionlimit(1500)


class image_validation:

    def __init__(self):
        self.source='Rovi'
        self.column_fieldnames = ["Rovi_id","Projectx_id","image_match",
                                                        "images_mismatch","Comment"]
        self.total=0
        self.Rovi_id=0
        self.show_type=''
        self.writer=''

    #TODO: Refresh
    def cleanup(self):
        self.px_images=[]
        self.rovi_images=[]
        self.image_match=''
        self.images_mismatch=[]
        self.px_images_present='False'
        self.rovi_images_present='False'
        self.comment=''

    def mysql_connection(self):
        self.connection_pxmapping=MySQLdb.Connection(host='192.168.86.10', port=3306, user='root', passwd='branch@123', db='testDB')
        self.px_mappingdb_cur=self.connection_pxmapping.cursor()    
 
    
    #TODO: one time call param
    def default_param(self):
        self.projectx_domain="testprojectx.caavo.com"
        self.prod_domain="api.caavo.com"
        self.host_IP='34.231.212.186:81'
        self.token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'

    def get_env_url(self):    
        self.programs_api='https://%s/programs?ids=%s&ott=true'
        self.rovi_image_api='http://%s/rovi/%s/image'  

    def getting_input_from_sheet(self,input_data,id_):
        #import pdb;pdb.set_trace()
        self.Rovi_id=str(input_data[id_][0])
        self.show_type=str(input_data[id_][1])
        print({"Rovi_id":self.Rovi_id,"show_type":self.show_type})     

    def collect_images_projectx(self,px_response):
        px_images=[]
        for data in px_response:
            if data["images"]:
                self.px_images_present='True'
                for images in data["images"]:
                    px_images.append(images["url"])
        return px_images 

    def collect_rovi_images(self,rovi_response):
        rovi_images=[]
        for images in rovi_response:
            rovi_images.append(images["imageUrl"]) 
        return rovi_images        

    def main(self,start_id,thread_name,end_id):
        #import pdb;pdb.set_trace()
        self.mysql_connection()
        self.get_env_url()
        inputFile="input/Rovi_ids_sheet"
        input_data=lib_common_modules().read_csv(inputFile)
        self.default_param()
        self.logger=lib_common_modules().create_log(os.getcwd()+'/logs/log_%s.txt'%thread_name)
        result_sheet='/output/output_file_%s_%s.csv'%(thread_name,datetime.date.today())
        output_file=lib_common_modules().create_csv(result_sheet)
        with output_file as mycsvfile:
            self.writer = csv.writer(mycsvfile,dialect="csv",lineterminator = '\n')
            self.writer.writerow(self.column_fieldnames)
            for id_ in range(start_id,end_id):
                self.cleanup()
                self.getting_input_from_sheet(input_data,id_)   
                px_id=lib_common_modules().getting_mapped_px_id(self.Rovi_id,self.show_type,self.source,
                                                                                self.px_mappingdb_cur )
                self.total+=1
                print ("\n") 
                print ({"Rovi_id":self.Rovi_id,"px_id":px_id,"Total":self.total})
                if px_id:
                    projectx_response=lib_common_modules().fetch_response_for_api(
                                     self.programs_api%(self.projectx_domain,px_id[0]),self.token,self.logger)
                    rovi_image_response=lib_common_modules().fetch_response_for_api(
                                     self.rovi_image_api%(self.host_IP,self.Rovi_id),self.token,self.logger)
                    if projectx_response:
                        self.px_images=self.collect_images_projectx(projectx_response)
                    if rovi_image_response:
                        self.rovi_images_present='True'
                        self.rovi_images=self.collect_rovi_images(rovi_image_response)    
                    if self.px_images and self.rovi_images_present=='True':
                        for images in self.px_images:
                            if images in self.rovi_images:
                                self.image_match='True'
                            else:
                                self.image_match='False'    
                                self.images_mismatch.append(images)

                    elif not self.px_images and self.rovi_images_present=='True':
                        self.comment="images_not_present_in_Projectx but present in Rovi"
                    elif not self.px_images and self.rovi_images_present=='False':
                        self.comment ='Both have no images'

                else:
                    self.comment ='this Rovi id not ingested in projectx' 

                self.writer.writerow([self.Rovi_id,px_id,self.image_match,
                                        self.images_mismatch,self.comment])                          
        self.px_mappingdb_cur.close()
        self.connection_pxmapping.close()


    def thread_pool(self):
        t1=threading.Thread(target=self.main,args=(1,"process - 1",10))
        t1.start()
        """t2=threading.Thread(target=self.main,args=(500,"process - 2",1000))
        t2.start()"""



if __name__ == "__main__": 
    image_validation().thread_pool()      

import logging    
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
import urllib2
import json
import httplib
sys.setrecursionlimit(1500)

class lib_modules:

    #TODO: to read CSV
    def read_csv(self,inputFile):
        input_file = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
        reader = csv.reader(input_file)
        input_data=list(reader)    
        return input_data

    #TODO: creating file for writing
    def create_csv(self,result_sheet):
        if (os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file

    #TODO: create log file
    def create_log(self,log_file):
        #import pdb;pdb.set_trace()
        self.logger = logging.getLogger()
        logging.basicConfig(filename=log_file,format=[],filemode='wa')
        self.logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(stream_handler)
        return self.logger

    #TODO: fetching response for the given API
    def fetch_response_for_api(self,api,token,logger):
        #import pdb;pdb.set_trace()
        try:    
            retry_count=0
            resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':token}))
            data = resp.read()
            data_resp = json.loads(data)
            return data_resp    
        except (Exception,URLError,httplib.BadStatusLine) as e:
            logger.debug ("\n Retrying...................",retry_count)
            logger.debug (["\n exception caught fetch_response_for_api Function..........",type(e),api])
            retry_count+=1
            if retry_count <=5:
                self.fetch_response_for_api(api,token,logger)   
            else:
                retry_count = 0

    #TODO: To check link expiry
    def link_expiry_check(self,expired_api,domain,link_id,service,expired_token,logger):
        #import pdb;pdb.set_trace()
        try:
            expired_api_response=self.fetch_response_for_api(expired_api%(domain,link_id,service),expired_token,logger)
            if expired_api_response["is_available"]==False:
                self.link_expired='False'
            else:
                self.link_expired='True'   
            return self.link_expired
        except (Exception,URLError,httplib.BadStatusLine) as e:
            logger.debug ("\n Retrying...................",retry_count)
            logger.debug (["\n exception caught link_expiry_check Function..........",type(e),expired_api,link_id,service])
            retry_count+=1
            if retry_count <=5:
                self.link_expiry_check(expired_api,domain,link_id,service,expired_token,logger)   
            else:
                retry_count = 0

class ingestion_script_modules:

    #TODO: creating file for writing
    def create_csv(self,result_sheet):
        if (os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('csv',lineterminator = '\n',skipinitialspace=True,escapechar='')
        output_file=open(os.getcwd()+result_sheet,"wa")
        return output_file

    #TODO: fetching response for the given API
    def fetch_response_for_api(self,api,token):
        #import pdb;pdb.set_trace()
        try:    
            retry_count=0
            resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':token}))
            data = resp.read()
            data_resp = json.loads(data)
            return data_resp    
        except (Exception,URLError,httplib.BadStatusLine) as e:
            print ("\n Retrying...................",retry_count)
            print (["\n exception caught fetch_response_for_api Function..........",type(e),api])
            retry_count+=1
            if retry_count <=5:
                self.fetch_response_for_api(api,token)   
            else:
                retry_count = 0 

    #TODO: to get duplicate source ids from mapping API
    def getting_duplicate_source_id(self,px_id,px_mapping_api,show_type,token,source):
        source_list=[]
        retry_count=0
        #import pdb;pdb.set_trace()
        try:
            for _id in px_id:
                data_resp_mapping=self.fetch_response_for_api(px_mapping_api%_id,token)
                for resp in data_resp_mapping:
                    if resp.get("data_source")==source and resp.get("sub_type")==show_type and resp.get("type")=='Program':
                        source_list.append(resp.get("sourceId"))
            return source_list
        except (Exception,URLError,httplib.BadStatusLine) as e:
            print ("\n Retrying...................",retry_count)
            print (["\n exception caught getting_duplicate_source_id Function..........",type(e),px_mapping_api])
            retry_count+=1
            if retry_count <=5:
                self.getting_duplicate_source_id(px_id,px_mapping_api,show_type,token,source)   
            else:
                retry_count = 0

    #TODO: getting px_ids form source_mapping API
    def getting_px_ids(self,source_mapping_api,_id,show_type,source,token):            
        #import pdb;pdb.set_trace()
        retry_count=0
        projectx_id=[]
        try:
            data_response_api=self.fetch_response_for_api(source_mapping_api%(_id,show_type),token)
            if data_response_api:
                for data in data_response_api:
                    if data["data_source"]==source and data["type"]=="Program" and data["sub_type"]==show_type:
                        projectx_id.append(data["projectxId"])
                return projectx_id 
            else:
                return projectx_id                               
        except (Exception,HTTPError,urllib2.URLError,socket.error) as e:
            retry_count+=1
            print ("Exception caught............",type(e),source_mapping_api,_id,show_type,source,token)
            print ("Retrying........",retry_count)
            if retry_count<=5:
                self.getting_px_ids(source_mapping_api,_id,show_type,source,token)            
            else:
                retry_count=0               



               
    



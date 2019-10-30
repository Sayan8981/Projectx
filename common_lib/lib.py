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
               
    



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

class lib_common_modules:

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

    #TODO: fetching response for the given API using logger
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

    #TODO: To check link expiry with logger
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

    #TODO: To check link expiry with logger
    def link_expiry_check(self,expired_api,domain,link_id,service,expired_token):
        #import pdb;pdb.set_trace()
        try:
            expired_api_response=self.fetch_response_for_api(expired_api%(domain,link_id,service),expired_token)
            if expired_api_response["is_available"]==False:
                self.link_expired='False'
            else:
                self.link_expired='True'   
            return self.link_expired
        except (Exception,URLError,httplib.BadStatusLine) as e:
            print ("\n Retrying...................",retry_count)
            print (["\n exception caught link_expiry_check Function..........",type(e),expired_api,link_id,service])
            retry_count+=1
            if retry_count <=5:
                self.link_expiry_check(expired_api,domain,link_id,service,expired_token)   
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

class mapping_script_modules:

    def __init__(self):
        self.multiple_mapped_count=0
        self.mapped_count=0
        self.not_mapped_count=0
        self.rovi_id_not_ingested_count=0
        self.source_id_not_ingested_count=0
        self.both_source_not_ingested_count=0
        self.id_not_present_in_DB_count=0

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

    def getting_px_ids(self,source_mapping_api,source_id,rovi_mapping_api,rovi_id,source,show_type,token):
        #import pdb;pdb.set_trace()
        retry_count=0
        source_projectx_id=[]
        rovi_projectx_id=[]
        try:
            url_rovi=rovi_mapping_api%rovi_id
            data_resp_rovi=self.fetch_response_for_api(url_rovi,token)
            url_source=source_mapping_api%(source_id,source,show_type)
            data_resp_source=self.fetch_response_for_api(url_source,token)

            for ii in data_resp_rovi:
                if ii["data_source"]=="Rovi" and ii["type"]=="Program":
                    rovi_projectx_id.append(ii["projectxId"])
            for jj in data_resp_source:
                if jj["data_source"]==source and jj["type"]=="Program" and jj["sub_type"]==show_type:
                    source_projectx_id.append(jj["projectxId"])
            print("\n")
            print ({"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id})
            #TODO: return px_ids
            return {"source_projectx_id":source_projectx_id,"rovi_projectx_id":rovi_projectx_id}
        except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,urllib2.URLError) as e:
            retry_count+=1
            print ("Exception caught ........................",type(e),source_id,rovi_id)
            if retry_count<=5:
                self.getting_px_ids(source_mapping_api,source_id,rovi_mapping_api,rovi_id,source,show_type,token)
            else:
                retry_count=0

    def to_check_presence_different_source_id(self,data_mapped_resp,dev_rovi_px_mapped,source_id,source,show_type):
        another_source_id_present=''
        for mapped in data_mapped_resp:
            if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
                dev_rovi_px_mapped.append({'rovi_projectx_id_mapped':"rovi_id:"+str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})

            elif mapped.get("type")=="Program" and mapped.get("data_source")==source and mapped.get("sub_type")==show_type:
                dev_rovi_px_mapped.append({"sourceId":str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})
                if mapped.get("sourceId")!=str(source_id):
                    another_source_id_present='True'
                else:
                    another_source_id_present='False' 
        return {"another_source_id_present":another_source_id_present,"dev_rovi_px_mapped":dev_rovi_px_mapped}

    def to_check_presence_different_rovi_id(self,data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,show_type):
        another_rovi_id_present=''
        for mapped in data_mapped_resp_source:
            if mapped.get("type")=='Program' and mapped.get("data_source")=='Rovi':
                dev_source_px_mapped.append({'source_projectx_id_mapped':"rovi_id:"+str(mapped.get("sourceId")+','+'Map_reason:'+str(mapped.get("map_reason")))})
                #TODO: taking decision
                if mapped.get("sourceId")!=str(rovi_id):
                    another_rovi_id_present='True'
                else:
                    another_rovi_id_present='False'

            elif mapped.get("type")=="Program" and mapped.get("data_source")==source and mapped.get("sub_type")==show_type:
                dev_source_px_mapped.append({"sourceId":str(mapped.get("sourceId"))})                   

        return {"another_rovi_id_present":another_rovi_id_present,"dev_source_px_mapped":dev_source_px_mapped}      

    def to_check_variant_parent_id(self,projectx_api_response,px_array,px_variant_id):
        variant_present='False'        
        for kk in projectx_api_response:
            if kk.get("variant_parent_id") is not None:  
                px_variant_id.append(kk.get("variant_parent_id"))
        for id_ in px_array:
            if id_ in px_variant_id:
                variant_present='True'
                break
        return variant_present  

    def to_check_series_match(self,projectx_api_response,px_series_id):
        series_id_match='False'
        for kk in projectx_api_response:
            if kk.get("series_id") not in px_series_id:
                px_series_id.append(kk.get("series_id"))
            else:
                series_id_match='True'
        return {"series_id_match":series_id_match,"px_series_id":px_series_id}              

    def checking_mapping_series(self,source_sm_id,rovi_sm_id,source_projectx_id,rovi_projectx_id,writer,source,token
                                 ,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking):
        #import pdb;pdb.set_trace()
        dev_rovi_px_mapped=[]
        dev_source_px_mapped=[]
        px_series_id=[]
        px_array=[]
        px_variant_id=[]

        #TODO: condition 1 , to check multiple projectx ids for a source
        if len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
            self.multiple_mapped_count=self.multiple_mapped_count+1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'  
                else:
                    comment= 'Multiple ingestion for same content of rovi' 
                    status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of %s'%source
                else:
                    comment= 'Fail:Multiple ingestion for same content of %s'%source
                    status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id:
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        comment= 'Fail:Multiple ingestion for same content of both sources'   
                        status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
                comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
                status='Fail'
            elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
                comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
                status='Fail'            
            writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_%s"%source:source_projectx_id,
                                                                                                   "Comment":comment,"Result of mapping series":status})
        #TODO: condition 2, to check both projectx ids are same or not 
        elif len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
            if rovi_projectx_id == source_projectx_id:
                self.mapped_count=self.mapped_count+1
                writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_%s"%source:str(source_projectx_id[0]),
                                                                                                      "Comment":'Pass',"Result of mapping series":'Pass'})
            else:
                #TODO: to check the mapping of rovi_projectx_id from mapping API
                projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_sm_id,source,'SM')        
                #TODO: To check the mapping of source_projetx_id from mapping API
                projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_sm_id,source,'SM')
                #TODO: checking variant parent id for PX_ids
                px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                variant_present=self.to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id)

                self.not_mapped_count=self.not_mapped_count+1
                writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id[0]),"projectx_id_%s"%source:str(source_projectx_id[0]),
                                 "Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],"another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],
                                 "another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,"Comment":'Fail',"Result of mapping series":'Fail'})
        #TODO: condition 3, to check rovi_id ingested
        elif len(source_projectx_id)==1 and not rovi_projectx_id:
            self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":'Nil',"projectx_id_%s"%source:source_projectx_id,
                                                                   "Comment":'rovi_id not ingested',"Result of mapping series":'Fail'})
        #TODO: condition 4, to check source id ingested and checking for another source_ids
        elif len(rovi_projectx_id)==1 and not source_projectx_id:
            retry_count=0
            try:
                duplicate_api=api_duplicate_checking%(source_sm_id,source)
                data_resp_url=self.fetch_response_for_api(duplicate_api,token)
                if data_resp_url==[]:
                    self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                    writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_%s"%source:'Nil',
                                                                 "Comment":'source_id not ingested',"Result of mapping series":'Fail'})
                else:
                    for ll in data_resp_url:
                        if ll.get("projectxId") not in source_projectx_id:
                            source_projectx_id.append(ll.get("projectxId"))
                    if rovi_projectx_id[0] in source_projectx_id:
                        self.mapped_count=self.mapped_count+1        
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id),"projectx_id_%s"%source:str(source_projectx_id),
                                                                                                         "Comment":'Pass',"Result of mapping series":'Pass'})
                    elif len(source_projectx_id)==1:
                        #TODO: to check the mapping of rovi_projectx_id from mapping API
                        projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                        data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                        another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_sm_id,source,'SM') 
                        #TODO: To check the mapping of source_projetx_id from mapping API
                        projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                        data_mapped_resp1=self.fetch_response_for_api(projectx_mapping_api,token)
                        another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_sm_id,source,'SM')
                        #TODO: checking variant parent id for PX_ids
                        px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                        projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                        projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                        variant_present=self.to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id)

                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":str(rovi_projectx_id),
                                        "projectx_id_%s"%source:str(source_projectx_id),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                        "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,
                                                             "Comment":'Fail',"Result of mapping series":'Fail'})
                    elif len(source_projectx_id)>1:
                        self.multiple_mapped_count=self.multiple_mapped_count+1
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_%s"%source:'Nil',
                                                                                          "Comment":'multiple source_px_id from duplicate API',"Result of mapping series":'Fail'})
                    else:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":rovi_projectx_id,"projectx_id_%s"%source:'Nil',
                                                                                          "Comment":'source_id not ingested',"Result of mapping series":'Fail'})    

            except (Exception,httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError) as e:
                retry_count+=1
                print ("exception caught ...........................................",type(e),source_sm_id,rovi_sm_id)
                if retry_count<=5:
                    self.checking_mapping_series(source_sm_id,rovi_sm_id,source_projectx_id,rovi_projectx_id,writer,source,token
                                 ,projectx_mapping_api,projectx_preprod_api,api_duplicate_checking)
                else:
                    retry_count=0    

        #TODO: condition 5, to check both source id ingested
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
            self.both_source_not_ingested_count=self.both_source_not_ingested_count+1
            writer.writerow({"%s_sm_id"%source:source_sm_id,"Rovi_sm_id":rovi_sm_id,"projectx_id_rovi":'',"projectx_id_%s"%source:'',"Comment":'both ids not ingested',
                                                                                                   "Result of mapping series":'N.A.'})
        print("\n")
        print("multiple mapped: ",self.multiple_mapped_count ,"mapped :",self.mapped_count ,"Not mapped :", self.not_mapped_count, 
              "rovi id not ingested :", self.rovi_id_not_ingested_count, "source Id not ingested : ",self.source_id_not_ingested_count, 
               "both not ingested: ",self.both_source_not_ingested_count)   

    #this function works only for Guidebox mapping
    def checking_mapping_episodes(self,source_id,rovi_id,source_sm_id,episode_title,ozoneepisodetitle,OzoneOriginalEpisodeTitle
                              ,scheme,rovi_series_id,rovi_projectx_id,source_projectx_id,source_projectx_id_sm,writer,source,
                              token,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking
                             ,api_check_presence_data,projectx_preprod_api_episodes):
        #import pdb;pdb.set_trace()
        dev_rovi_px_mapped=[]
        dev_source_px_mapped=[]
        px_series_id=[]
        px_array=[]   
        #TODO: condition 1, to check both projectx ids are same or not
        if len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
            if rovi_projectx_id == source_projectx_id:
                self.mapped_count+=1
                writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                 "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                         "projectx_id_%s"%source:str(source_projectx_id[0]),"Episode mapping":'Pass',"Comment":'Pass'})

            else:
                #import pdb;pdb.set_trace()
                projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'SE')        

                projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,'SE')    

                px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                series_id_match_status=self.to_check_series_match(projectx_api_response,px_series_id)

                self.not_mapped_count+=1
                writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                 "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                 "projectx_id_%s"%source:str(source_projectx_id[0]),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                 "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"series_id_match":series_id_match_status["series_id_match"],"Px_series_id":series_id_match_status["px_series_id"],
                                  "Episode mapping":'Fail',"Comment":'Fail'})
        #TODO: condition 2 , to check multiple projectx ids for a source        
        elif len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
            self.multiple_mapped_count+self.multiple_mapped_count+1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id[0] in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'
                else:
                    status='Fail'
                    comment='Fail:Multiple ingestion for same content of rovi'  
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of %s'%source
                else:
                    status='Fail'
                    comment=  'Fail:Multiple ingestion for same content of %s'%source  
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id: 
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        status='Fail'
                        comment= 'Fail:Multiple ingestion for same content of both sources'   
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
                comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
                status='Fail'
                self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
                comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
                status='Fail'
                self.ource_id_not_ingested_count=self.source_id_not_ingested_count+1

            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                            "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":rovi_projectx_id,
                                                                         "projectx_id_%s"%source:source_projectx_id,"Comment":comment,"Episode mapping":status})
        #TODO: condition 3, to check rovi_id ingested
        elif len(source_projectx_id)==1 and not rovi_projectx_id:
            self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                            "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'Nil',
                            "projectx_id_%s"%source:source_projectx_id,"Episode mapping":'N.source_projectx_id',"Comment":'No ingestion of rovi_id of episodes and multiple ingestion for GB_id'})
        #TODO: condition 4, to check both source id ingested
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
            self.both_source_not_ingested_count=self.both_source_not_ingested_count+1
            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                            "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":'',
                                      "projectx_id_%s"%source:'',"Episode mapping":'N.source_projectx_id',"Comment":'No Ingestion of both sources'})
        #TODO: condition 5, to check source id ingested and checking for another source_ids    
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==1:
            retry_count=0
            try:
                duplicate_api=api_duplicate_checking%(source_sm_id,source)
                data_resp_url=self.fetch_response_for_api(duplicate_api,token)
                if data_resp_url==[]:
                    id_api=api_check_presence_data%(source,source_sm_id)
                    data_resp_url_source=self.fetch_response_for_api(id_api,token)
                    if data_resp_url_source==True:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1 
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                        "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                               "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'})
                    else:
                        self.id_not_present_in_DB_count=self.id_not_present_in_DB_count+1
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                         "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                     "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'Not present of source_id of series in GB DB'})  
                else:
                    for ll in data_resp_url:
                        if ll.get("projectxId") not in source_projectx_id_sm:
                            source_projectx_id_sm.append(ll.get("projectxId"))

                    for sm_id in source_projectx_id_sm:       
                        projectx_url=projectx_preprod_api_episodes%sm_id
                        data_resp_url=self.fetch_response_for_api(projectx_url,token)
                        if data_resp_url!=[]:
                            for hh in data_resp_url:
                                source_projectx_id.append(hh.get("id"))
                           
                    if source_projectx_id!=[]:
                        if rovi_projectx_id[0] in source_projectx_id:
                            self.mapped_count=self.mapped_count+1
                            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                            "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id),
                                                                                  "projectx_id_%s"%source:str(rovi_projectx_id),"Episode mapping":'Pass',"Comment":'Pass'})
                        else:
                            projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                            data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                            another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'SE')
             
                            self.not_mapped_count=self.not_mapped_count+1
                            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                             "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id),
                                             "projectx_id_%s"%source:['not found match id'],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                            "another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"Episode mapping":'Fail',"Comment":'Fail'})       
                    else:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1 
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"episode_title":episode_title,
                                         "Scheme":scheme,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                                   "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'}) 

            except (httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,Exception) as e:
                retry_count+=1
                print ("exception caught ...........................................",type(e),source_id,rovi_id)
                print("\n")
                print ("Retrying.............",retry_count)
                if retry_count<=5:
                    self.checking_mapping_episodes(source_id,rovi_id,source_sm_id,episode_title,ozoneepisodetitle,OzoneOriginalEpisodeTitle
                                 ,scheme,rovi_series_id,rovi_projectx_id,source_projectx_id,source_projectx_id_sm,
                                writer,source,token,projectx_mapping_api,projectx_preprod_api,api_duplicate_checking,api_check_presence_data,projectx_preprod_api_episodes)
                else:
                   retry_count=0    
        
        print("\n")
        print("multiple mapped: ",self.multiple_mapped_count,"mapped :",self.mapped_count ,
              "Not mapped :", self.not_mapped_count, "rovi id not ingested :", self.rovi_id_not_ingested_count, 
               "source Id not ingested : ",self.source_id_not_ingested_count, "both not ingested: ",self.both_source_not_ingested_count, 
                 "id not present in db:" ,self.id_not_present_in_DB_count)                    

    # this func works for sources mapping except Guidebox 
    def checking_mapping_episodes_(self,source_id,rovi_id,source_sm_id,rovi_series_id,rovi_projectx_id,source_projectx_id,
                              source_projectx_id_sm,writer,source,token,projectx_mapping_api_,projectx_preprod_api,
                              api_duplicate_checking,projectx_preprod_api_episodes):
        #import pdb;pdb.set_trace()
        dev_rovi_px_mapped=[]
        dev_source_px_mapped=[]
        px_series_id=[]
        px_array=[]
        #TODO: condition 1, to check both projectx ids are same or not
        if len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
            if rovi_projectx_id == source_projectx_id:
                self.mapped_count=self.mapped_count+1
                writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                        "projectx_id_%s"%source:str(source_projectx_id[0]),"Episode mapping":'Pass',"Comment":'Pass'})

            else:
                #import pdb;pdb.set_trace()
                projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'SE')    

                projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,'SE')    

                px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                series_id_match_status=self.to_check_series_match(projectx_api_response,px_series_id)

                self.not_mapped_count=self.not_mapped_count+1
                writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                "projectx_id_%s"%source:str(source_projectx_id[0]),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                 "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"series_id_match":series_id_match_status["series_id_match"],
                                 "Px_series_id":series_id_match_status["px_series_id"],"Episode mapping":'Fail',"Comment":'Fail'})
        #TODO: condition 2 , to check multiple projectx ids for a source        
        elif len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
            self.multiple_mapped_count=self.multiple_mapped_count+1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id[0] in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'
                else:
                    status='Pass'
                    comment='Fail:Multiple ingestion for same content of rovi'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of %s'%source
                else:
                    status='Fail'
                    comment=   'Fail:Multiple ingestion for same content of %s'%source 
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id: 
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        status='Fail'
                        comment= 'Fail:Multiple ingestion for same content of both sources'   
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
                comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
                status='Fail'
                self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
                comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
                status='Fail'            
                self.source_id_not_ingested_count=self.source_id_not_ingested_count+1

            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":rovi_projectx_id,
                                                                 "projectx_id_%s"%source:source_projectx_id,"Comment":comment,"Episode mapping":status})
        #TODO: condition 3, to check both source id ingested
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
            self.both_source_not_ingested_count=self.both_source_not_ingested_count+1
            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":'',"projectx_id_%s"%source:'',
                                "Episode mapping":'Fail',"Comment":'No Ingestion of both sources'})
        #TODO: condition 4, to check rovi_id ingested
        elif len(source_projectx_id)==1 and len(rovi_projectx_id)==0:
            self.rovi_id_not_ingested_count=self.rovi_id_not_ingested_count+1
            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":'',
                               "projectx_id_%s"%source:str(source_projectx_id[0]),"Episode mapping":'Fail',"Comment":'No Ingestion of rovi_id of episode'})
        #TODO: condition 5, to check source id ingested and checking for another source_ids    
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==1:
            retry_count=0
            try:
                duplicate_api=api_duplicate_checking%(source_sm_id,source)
                data_resp_url=self.fetch_response_for_api(duplicate_api,token)
                if data_resp_url==[]:
                    self.source_id_not_ingested_count=self.source_id_not_ingested_count+1 
                    writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                        "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'}) 

                else:
                    for ll in data_resp_url:
                        if ll.get("projectx_id") not in source_projectx_id_sm:
                            source_projectx_id_sm.append(ll.get("projectx_id"))
                    for sm_id in source_projectx_id_sm:       
                        projectx_url=projectx_preprod_api_episodes%sm_id
                        data_resp_url=self.fetch_response_for_api(projectx_url,token)
                        if data_resp_url!=[]:
                            for hh in data_resp_url:
                                source_projectx_id.append(hh.get("id"))
                           
                    if source_projectx_id!=[]:
                        if rovi_projectx_id[0] in source_projectx_id:
                            self.mapped_count=self.mapped_count+1
                            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id),
                                                                              "projectx_id_%s"%source:str(rovi_projectx_id),"Episode mapping":'Pass',"Comment":'Pass'})
                        else:
                            projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                            data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                            another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'SE')
             
                            self.not_mapped_count=self.not_mapped_count+1
                            writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id),
                                            "projectx_id_%s"%source:['not found match id'],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                            "another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"Episode mapping":'Fail',"Comment":'Fail'})       
                    else:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1 
                        writer.writerow({"%s_sm_id"%source:source_sm_id,"%s_id"%source:source_id,"Rovi_id":rovi_id,"Rovi_SM_id":rovi_series_id,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                                                   "projectx_id_%s"%source:'',"Episode mapping":'Fail',"Comment":'No Ingestion of source_id of episode'})        
            
            except (httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError,Exception) as e:
                retry_count+=1
                print ("exception caught ................................................",type(e),source_id,rovi_id)
                print("\n")
                print ("Retrying.............",retry_count)
                if retry_count<=5:
                    self.checking_mapping_episodes(source_id,rovi_id,source_sm_id,rovi_series_id,rovi_projectx_id,source_projectx_id,source_projectx_id_sm,writer,source,
                              token,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking,projectx_preprod_api_episodes)
                else:
                   retry_count=0  

        print("\n")
        print("multiple mapped: ",self.multiple_mapped_count,"mapped :",self.mapped_count ,"Not mapped :", self.not_mapped_count, 
              "rovi id not ingested :", self.rovi_id_not_ingested_count, "source Id not ingested : ",self.source_id_not_ingested_count,
                "both not ingested: ",self.both_source_not_ingested_count)

    #TODO: checking mapping movies for the given sources(ROVI, others)
    def checking_mapping_movies(self,source_id,rovi_id,movie_name,release_year,source_projectx_id,rovi_projectx_id,writer
                               ,source,token,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking):
        dev_rovi_px_mapped=[]
        dev_source_px_mapped=[]
        px_series_id=[]
        px_array=[]
        px_variant_id=[]
        #TODO: condition 1 , to check multiple projectx ids for a source
        if len(rovi_projectx_id)>1 or len(source_projectx_id)>1:
            self.multiple_mapped_count+=1
            status='Nil'
            comment=''
            if len(source_projectx_id)==1 and len(rovi_projectx_id)>1:
                if source_projectx_id in rovi_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of rovi'  
                else:
                    comment= 'Multiple ingestion for same content of rovi' 
                    status='Fail' 
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==1:
                if rovi_projectx_id[0] in source_projectx_id:
                    status='Pass'
                    comment='Multiple ingestion for same content of %s'%source
                else:
                    comment= 'Fail:Multiple ingestion for same content of %s'%source
                    status='Fail'   
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)>1:
                for x in source_projectx_id:
                    if x in rovi_projectx_id:
                        status='Pass'
                        comment='Multiple ingestion for same content of both sources'
                        break;
                    else:
                        comment= 'Fail:Multiple ingestion for same content of both sources'   
                        status='Fail'
            elif len(source_projectx_id)>1 and len(rovi_projectx_id)==0:
                comment='Fail:Multiple ingestion for same content of source and rovi_id not ingested'
                status='Fail'
            elif len(rovi_projectx_id)>1 and len(source_projectx_id)==0:
                comment= 'Fail:Multiple ingestion for same content of rovi and source_id not ingested'
                status='Fail'
            writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":rovi_projectx_id,
                               "projectx_id_%s"%source:source_projectx_id,"Comment":comment,"Result of mapping":status})    

        #TODO: condition 2, to check both projectx ids are same or not
        elif len(rovi_projectx_id)==1 and len(source_projectx_id)==1:
            if rovi_projectx_id == source_projectx_id:
                self.mapped_count+=1
                writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                             "projectx_id_%s"%source:str(source_projectx_id[0]),"Comment":'Pass',"Result of mapping":'Pass'})

            else:
                self.not_mapped_count+=1
                #import pdb;pdb.set_trace()
                #TODO: to check the mapping of rovi_projectx_id from mapping API
                projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'MO')   
                #TODO: To check the mapping of source_projetx_id from mapping API
                projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                another_rovi_id_present_status=self.to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,'MO')
                px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                #TODO: checking variant parent id for PX_ids
                projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                variant_present=self.to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id)
                               
                writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id[0]),
                                      "projectx_id_%s"%source:str(source_projectx_id[0]),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                     "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,
                                                         "Comment":'Fail',"Result of mapping":'Fail'})

        #TODO: condition 3, to check rovi_id ingested 
        elif len(source_projectx_id)==1 and not rovi_projectx_id:
            self.rovi_id_not_ingested_count+=1
            writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":'Nil',
                              "projectx_id_%s"%source:str(source_projectx_id),"Comment":'rovi_id not ingested',"Result of mapping":'Fail'})

        #TODO: condition 4, to check source id ingested and checking for another source_ids
        elif len(rovi_projectx_id)==1 and not source_projectx_id:
            try:
                retry_count=0
                #import pdb;pdb.set_trace()
                #TODO: to check duplicate id from source mapping API
                duplicate_api=api_duplicate_checking%(eval(source_id),source)
                data_resp_url=self.fetch_response_for_api(duplicate_api,token)
                if data_resp_url==[]:
                    self.source_id_not_ingested_count+=1
                    writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                               "projectx_id_%s"%source:'Nil',"Comment":'source_id not ingested',"Result of mapping":'Fail'}) 
                else: 
                    #TODO: taking source_px_id from duplicate API                        
                    for ll in data_resp_url:
                        if ll.get("projectxId") not in source_projectx_id:
                            source_projectx_id.append(ll.get("projectxId"))
                    if rovi_projectx_id[0] in source_projectx_id:
                        self.mapped_count+=1        
                        writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                             "projectx_id_%s"%source:str(source_projectx_id),"Comment":'Pass',"Result of mapping":'Pass'})   
                    elif len(source_projectx_id)==1:
                        self.not_mapped_count+=1
                        #TODO: checking the mapping of rovi_projectx_id
                        projectx_mapping_api=projectx_mapping_api_%rovi_projectx_id[0]
                        data_mapped_resp=self.fetch_response_for_api(projectx_mapping_api,token)
                        another_source_id_present_status=self.to_check_presence_different_source_id(data_mapped_resp,dev_rovi_px_mapped,source_id,source,'MO')
                        #TODO: checking the mapping of source_projectx_id
                        projectx_mapping_api=projectx_mapping_api_%source_projectx_id[0]
                        data_mapped_resp_source=self.fetch_response_for_api(projectx_mapping_api,token)
                        another_rovi_id_present_status=to_check_presence_different_rovi_id(data_mapped_resp_source,dev_source_px_mapped,rovi_id,source,'MO')   
                        #TODO : to check variant parent ids for Px_ids
                        px_array=[source_projectx_id[0],rovi_projectx_id[0]]
                        projectx_api=projectx_preprod_api%'{}'.format(",".join([str(i) for i in px_array]))
                        projectx_api_response=self.fetch_response_for_api(projectx_api,token)
                        variant_present=self.to_check_variant_parent_id(projectx_api_response,px_array,px_variant_id) 

                        writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                        "projectx_id_%s"%source:str(source_projectx_id),"Dev_%s_px_mapped"%source:another_rovi_id_present_status["dev_source_px_mapped"],"Dev_rovi_px_mapped":another_source_id_present_status["dev_rovi_px_mapped"],
                                        "another_rovi_id_present":another_rovi_id_present_status["another_rovi_id_present"],"another_%s_id_present"%source:another_source_id_present_status["another_source_id_present"],"variant_present":variant_present,
                                                                   "Comment":'Fail',"Result of mapping":'Fail',})
                    elif len(source_projectx_id)>1:  
                        self.multiple_mapped_count=self.multiple_mapped_count+1  
                        writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                           "projectx_id_%s"%source:'Nil',"Comment":'multiple source_px_id from duplicate API',"Result of mapping":'Fail'})    
                    else:
                        self.source_id_not_ingested_count=self.source_id_not_ingested_count+1
                        writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":str(rovi_projectx_id),
                                           "projectx_id_%s"%source:'Nil',"Comment":'Source_id not ingested',"Result of mapping":'Fail'})                  
                        
            except (httplib.BadStatusLine,urllib2.HTTPError,socket.error,URLError) as e:
                print ("exception caught ..................................................",type(e),source_id,rovi_id)
                if retry_count<=5:
                    self.checking_mapping_movies(source_id,rovi_id,movie_name,release_year,source_projectx_id,rovi_projectx_id,writer
                               ,source,token,projectx_mapping_api_,projectx_preprod_api,api_duplicate_checking)
                else:
                   retry_count=0

        #TODO: condition 5, to check both source id ingested
        elif len(source_projectx_id)==0 and len(rovi_projectx_id)==0:
            self.both_source_not_ingested_count+=1
            writer.writerow({"%s_id"%source:source_id,"Rovi_id":rovi_id,"movie_name":movie_name,"release_year":release_year,"projectx_id_rovi":'',"projectx_id_%s"%source:'',
                                                              "Comment":'both ids not ingested',"Result of mapping":'N.A'})

        print("multiple mapped: ",self.multiple_mapped_count ,"mapped :",self.mapped_count ,"Not mapped :", self.not_mapped_count,
              "rovi id not ingested :", self.rovi_id_not_ingested_count, "source Id not ingested : ",self.source_id_not_ingested_count, "both not ingested: ",self.both_source_not_ingested_count)    


class duplicate_script_modules:

    def search_api_call_response(self, px_id, source, link_expired, writer,writer_credit_match_false,title):
        #import pdb;pdb.set_trace() 
        next_page_url=""
        duplicate=""
        data_resp_search=dict()

        search_api=initialization_file.projectx_preprod_search_api%urllib2.quote(title)
        data_resp_search = lib_common_modules().fetch_response_for_api(search_api,initialization_file.token)
         
        while data_resp_search.get("results"):
            ## TODO: We are taking only the last response. We should take all  
            for nn in data_resp_search.get("results"):
                if nn.get("action_type")=="ott_search" and (nn.get("results")==[] or nn.get("results")):
                    next_page_url=nn.get("next_page_url")
                    if next_page_url is not None: 
                        search_api=initialization_file.domain_name+next_page_url.replace(' ',"%20")
                        data_resp_search = lib_common_modules().fetch_response_for_api(search_api,initialization_file.token)
                        #self.search_api_response_validation(data_resp_search, source, px_id, link_expired, writer, duplicate,writer_credit_match_false)
                    else:    
                        data_resp_search={"results":[]}
                else:
                    data_resp_search={"results":[]}
                return data_resp_search            
    
    class gb_link_id_extract:

        def getting_purchase_link_ids(data_GB_resp):
            #import pdb;pdb.set_trace()
            gb_purchase_web_link=''
            gb_purchase_web_source=''
            gb_purchase_web_id=[]
                    
            for link in range(0,len(data_GB_resp.get("purchase_web_sources"))):#data_GB_resp.get("purchase_web_sources")
                gb_purchase_web_source=data_GB_resp.get("purchase_web_sources")[link].get("source").encode('utf-8')
                gb_purchase_web_link=data_GB_resp.get("purchase_web_sources")[link].get("link").encode('utf-8')
                if 'amazon_prime' in gb_purchase_web_source or 'amazon_buy' in gb_purchase_web_source:
                    gb_purchase_web_source='amazon'
                if 'netflix' in gb_purchase_web_source:
                    gb_purchase_web_source='netflixusa'
                if gb_purchase_web_source=='hbo':
                    gb_purchase_web_source='hbogo'
                if gb_purchase_web_source=='hbo_now':
                    gb_purchase_web_source='hbogo'
                if gb_purchase_web_source=='google_play':
                    gb_purchase_web_source='googleplay'
                if gb_purchase_web_source=='hulu_plus':
                    gb_purchase_web_source='hulu'
                if gb_purchase_web_source =='verizon_on_demand':
                    gb_purchase_web_source='verizon'
                if gb_purchase_web_source=='showtime_subscription':
                    gb_purchase_web_source='showtime'
                               
                if 'vuduapp' in gb_purchase_web_link:
                    gb_purchase_web_id.append(re.findall("\w+.*?", gb_purchase_web_link)[-1:][0])
                try:
                    if '//itunes.apple.com/us/tv-season' in gb_purchase_web_link:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[-2:-1][0]})
                except IndexError:
                    try:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[1:-2][0]})
                    except IndexError:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0]})
                try:
                    if '//itunes.apple.com/us/movie' in gb_purchase_web_link:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0:-2][2:][1]})
                except IndexError:
                    try:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[1:-2][1]})
                    except IndexError:
                        try:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+",gb_purchase_web_link)[0:-2][1:2][0]})
                        except IndexError:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0]})
                if '//www.amazon.com/gp' in gb_purchase_web_link:
                    gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\w+\d+\w+", gb_purchase_web_link)[0]})
                if '//click.linksynergy.com/' in gb_purchase_web_link:
                    gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+.*?", gb_purchase_web_link)[-1:][0]})
                if gb_purchase_web_id:
                    return gb_purchase_web_id
                    break
                    

        def getting_link_subscription_ids(data_GB_resp): 

            gb_subscription_web_source=''
            gb_subscription_web_id=[]
            #import pdb;pdb.set_trace()
            for link in range(0,len(data_GB_resp.get("subscription_web_sources"))):
                gb_subscription_web_source=data_GB_resp.get("subscription_web_sources")[link].get('source').encode('utf-8')
                gb_subscription_web_link=data_GB_resp.get("subscription_web_sources")[link].get('link').encode('utf-8')

                if 'amazon_prime' in gb_subscription_web_source or 'amazon_buy' in gb_subscription_web_source:
                    gb_subscription_web_source='amazon'
                if 'netflix' in gb_subscription_web_source:
                    gb_subscription_web_source='netflixusa'
                if gb_subscription_web_source=='hbo':
                    gb_subscription_web_source='hbogo'
                if gb_subscription_web_source=='hbo_now':
                    gb_subscription_web_source='hbogo'
                if gb_subscription_web_source=='google_play':
                    gb_subscription_web_source='googleplay'
                if gb_subscription_web_source=='hulu_plus':
                    gb_subscription_web_source='hulu'
                if gb_subscription_web_source =='verizon_on_demand':
                    gb_subscription_web_source='verizon'
                if gb_subscription_web_source=='showtime_subscription':
                    gb_subscription_web_source='showtime'               

                if 'vuduapp' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})
                                
                if 'aiv://aiv/play' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})
                if '//itunes.apple.com/us/movie' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\d+", gb_subscription_web_link)[0]})
                if '//www.amazon.com/gp' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\d+\w+", gb_subscription_web_link)[0]})
                if "www.cbs.com/shows" in gb_subscription_web_link:
                    try:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\d+\w+", gb_subscription_web_link)[0]})
                    except IndexError:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+", gb_subscription_web_link)[7]})
                if '//click.linksynergy.com/' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\d+", gb_subscription_web_link)[-1:][0]})
                if 'play.google' in gb_subscription_web_link:
                    try:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+-\w+.*?",gb_subscription_web_link)[0]})
                    except IndexError:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})                      
                if '//play.hbonow.com/' in gb_subscription_web_link:
                    try:
                        a10=re.findall("\w+.*?", gb_subscription_web_link)
                        gb_subscription_web_id.append({gb_subscription_web_source:':'.join(map(str, [a10[i] for i in range(5,9)]))  })
                    except IndexError:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_subscription_web_link)[0]})                         
                if 'netflix.com' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0]})
                if 'http://www.showtime.com/#' in gb_subscription_web_link:
                    gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0]})
                if 'http://www.hulu.com' in gb_subscription_web_link:
                    try:
                        a14=re.findall("\w+.*?",gb_subscription_web_link)[-5:]
                        gb_subscription_web_id.append({gb_subscription_web_source:'-'.join(map(str,[a14[i] for i in range(0,len(a14))]))})
                    except IndexError:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0] })
                if gb_subscription_web_id:
                    return gb_subscription_web_id
                    break

        def getting_link_tveverywhere_ids(data_GB_resp):

            gb_tv_everywhere_web_source=''
            gb_tv_everywhere_web_link=''
            gb_tv_everywhere_web_id=[]

              
            for link in range(0,len(data_GB_resp.get("tv_everywhere_web_sources"))):
                gb_tv_everywhere_web_source=data_GB_resp.get("tv_everywhere_web_sources")[link].get('source').encode('utf-8')
                gb_tv_everywhere_web_link=data_GB_resp.get("tv_everywhere_web_sources")[link].get('link').encode('utf-8')
                if 'amazon_prime' in gb_tv_everywhere_web_source or 'amazon_buy' in gb_tv_everywhere_web_source:
                    gb_tv_everywhere_web_source='amazon'
                if 'netflix' in gb_tv_everywhere_web_source:
                    gb_tv_everywhere_web_source='netflixusa'
                if gb_tv_everywhere_web_source=='hbo':
                    gb_tv_everywhere_web_source='hbogo'
                if gb_tv_everywhere_web_source=='hbo_now':
                    gb_tv_everywhere_web_source='hbogo'
                if gb_tv_everywhere_web_source=='starz_tveverywhere':
                    gb_tv_everywhere_web_source='starz'
                if "starz://play" in gb_tv_everywhere_web_link or "//www.starz.com/" in gb_tv_everywhere_web_link:
                    gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+.*?", gb_tv_everywhere_web_link)[-1:][0]})
                if "www.cbs.com/shows" in gb_tv_everywhere_web_link:
                    try: 
                        gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+\d+\w+", gb_tv_everywhere_web_link)[0]})
                    except IndexError:
                        gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+", gb_tv_everywhere_web_link)[7]})                       
                if gb_tv_everywhere_web_id:
                    return gb_tv_everywhere_web_id
                    break

        def getting_link_free_web_ids(data_GB_resp):

            gb_free_web_source=''
            gb_free_web_link=''
            gb_free_web_id=[]
                
            for link in range(0,len(data_GB_resp.get("free_web_sources"))):
                gb_free_web_source=data_GB_resp.get("free_web_sources")[link].get('source').encode('utf-8')
                gb_free_web_link=data_GB_resp.get("free_web_sources")[link].get('link').encode('utf-8')
                if 'amazon_prime' in gb_free_web_source or 'amazon_buy' in gb_free_web_source:
                    gb_free_web_source='amazon'
                if 'https://www.vudu.com' in gb_free_web_source:
                    gb_free_web_source='vudu'
                if '//www.amazon.com/gp' in gb_free_web_link:
                    gb_free_web_id.append({gb_free_web_source:re.findall("\w+\d+\w+", gb_free_web_link)[0]})    
                if 'https://www.vudu.com' in gb_free_web_link:
                    gb_free_web_id.append({gb_free_web_source:re.findall("\w+.*?",gb_free_web_link)[-3:][0]})
                if "//play.hbogo.com/feature" in gb_free_web_link:
                    try:
                        gb_free_web_id.append({gb_free_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_free_web_link)[0]})
                    except IndexError:
                        try:
                            a3=re.findall("\w+.*?", gb_free_web_link)
                            gb_free_web_id.append({gb_free_web_source:':'.join(map(str, [a3[i] for i in range(5,9)]))})
                        except IndexError:
                            gb_free_web_id.append({gb_free_web_source:re.findall("\w+.*?",gb_free_web_link)[-1:][0]})
                if "http://www.nbc.com" in gb_free_web_link:
                    gb_free_web_id.append({gb_free_web_source:re.findall("\d+",gb_free_web_link)[0]})                
                if gb_free_web_id:
                    return gb_free_web_id
                    break    
    



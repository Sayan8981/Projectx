
import sys
import os
import csv
from urllib2 import HTTPError,URLError
import socket
import datetime
import urllib2
import json
import httplib
import unidecode
sys.setrecursionlimit(1500)
import pinyin
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file
import getting_gb_link_id
import checking_mata_data
#from googletrans import Translator

class ProjectXMovies:
    #import pdb;pdb.set_trace()
    def __init__(self):
        self.id = 0
        self.movie_title = ''
        self.english_title = ''
        self.release_year = 0
        self.px_id=[]
        self.link_expired=''
        self.credit_match=''
        self.credit_array=[]
        self.link_details=[]
        self.count_rovi=0
        self.count_source=0
        self.count_guidebox=0
        self.comment_variant_parent_rovi_id=[]
        self.comment_variant_parent_id=[]
        self.comment_variant_parent_id_present=''

    def cleanup(self):
        #import pdb;pdb.set_trace()
        self.id = 0
        self.movie_title = ''
        self.english_title = ''
        self.release_year = 0
        self.px_id=[]
        self.link_expired=''
        self.credit_match=''
        self.credit_array=[]
        self.link_details=[]
        self.count_rovi=0
        self.count_source=0
        self.count_guidebox=0
        self.comment_variant_parent_rovi_id=[]
        self.comment_variant_parent_id=[]
        self.comment_variant_parent_id_present=''

    def fetch_from_cloud_for_id(self, api):
        #import pdb;pdb.set_trace()
        try:
            retry_count=0
            link = urllib2.Request(api)
            link.add_header('Authorization',initialization_file.token)
            link.add_header('User-Agent','Branch Fyra v1.0')
            resp = urllib2.urlopen(link)
            data = resp.read()
            data_resp = json.loads(data)
            ## TODO: Validate response before returning
            return data_resp
        except (Exception,URLError,httplib.BadStatusLine,socket.error,err) as e:
            print ("\n Retrying...................")
            print ("\n exception caught fetch_from_cloud_for_id Function..........",type(e),api,self.id)
            retry_count+=1
            if retry_count <=5:
                self.fetch_from_cloud_for_id(api)   
            else:
                retry_count = 0

    def expiry_check_response(self,expired_api):
        try:
            link = urllib2.Request(expired_api)
            link.add_header('Authorization',initialization_file.Token)
            link.add_header('User-Agent','Branch Fyra v1.0')
            resp = urllib2.urlopen(link)
            data = resp.read()
            data_resp = json.loads(data)
            retry_count=0
            ## TODO: Validate response before returning
            return data_resp
        except (Exception,URLError,httplib.BadStatusLine,socket.error,err) as e:
            print ("\n Retrying...................")
            print ("\n exception caught expiry_check_response Function..........",type(e),expired_api,self.id)
            retry_count+=1
            if retry_count <=5:
                self.expiry_check_response(expired_api)    
            else:
                retry_count=0
               
    def expiry_check(self, link_details):
        #import pdb;pdb.set_trace()
        # TODO: Verifying only the first index of link
        expired_api=initialization_file.preprod_version_expire_domain%(link_details[0].get(link_details[0].keys()[0]),link_details[0].keys()[0])
        return self.expiry_check_response(expired_api)

    def variant_parent_id_checking_px(self,data_resp_preprod,px_id):
        #import pdb;pdb.set_trace()
        for oo in data_resp_preprod:
            if oo.get("variant_parent_id") is not None:
                if oo.get("variant_parent_id") not in px_id:
                    self.comment_variant_parent_id_present='Different'
                    self.comment_variant_parent_id.append({'variant_parent_id':str(oo.get("variant_parent_id"))})  
                else:
                    self.comment_variant_parent_id_present='True'
                    self.comment_variant_parent_id.append({'variant_parent_id':str(oo.get("variant_parent_id"))})
            else:
                self.comment_variant_parent_id_present='Null'
        return [self.comment_variant_parent_id,self.comment_variant_parent_id_present]        
            
    def variant_parent_id_checking_rovi(self,data_resp_preprod,rovi_id):
        for oo in data_resp_preprod:
            if oo.get("variant_parent_id") is not None:
                if oo.get("variant_parent_id") not in rovi_id:
                    self.comment_variant_parent_id_present='Different'
                    self.comment_variant_parent_rovi_id.append({'variant_parent_id':str(oo.get("variant_parent_id"))})  
                else:
                    self.comment_variant_parent_id_present='True'
                    self.comment_variant_parent_rovi_id.append({'variant_parent_id':str(oo.get("variant_parent_id"))})
            else:
                self.comment_variant_parent_id_present='Null'
        return [self.comment_variant_parent_rovi_id,self.comment_variant_parent_id_present]        

    def reverse_api_extract(self, link_details,source):
        #import pdb;pdb.set_trace()
        ## TODO: Only first index has been referred.
        reverse_api=initialization_file.reverse_api_domain%(link_details[0].get(link_details[0].keys()[0]),link_details[0].keys()[0])
        reverse_api_resp=self.fetch_from_cloud_for_id(reverse_api)

        for kk in reverse_api_resp:
            if kk.get("data_source")=='Rovi' and kk.get("type")=='Program' and kk.get("sub_type")=='ALL':
                if kk.get("projectxId") not in self.px_id:      
                    self.px_id.append(kk.get("projectxId"))
            if kk.get("data_source")==source and kk.get("type")=='Program' and kk.get("sub_type")=='MO':
                if kk.get("projectxId") not in self.px_id:      
                    self.px_id.append(kk.get("projectxId"))

    def duplicate_results(self,search_px_id__tmp,source,writer,writer1):
        rovi_id=[]
        rovi_mapping=[]
        source_mapping=[]
        guidebox_mapping=[]

        if (self.credit_match=='False' or self.credit_match=='') and len(search_px_id__tmp)==2:
            #import pdb;pdb.set_trace()
            px_link=initialization_file.projectx_preprod_domain %'{}'.format(",".join([str(i) for i in search_px_id__tmp]))
            data_resp_credits=self.fetch_from_cloud_for_id(px_link)
            #import pdb;pdb.set_trace()
            for uu in data_resp_credits:
                if uu.get("credits"):
                    for tt in uu.get("credits"):
                        self.credit_array.append(unidecode.unidecode(tt.get("full_credit_name")))        
            if self.credit_array:
                for cc in self.credit_array:
                    if self.credit_array.count(cc)>1:
                        self.credit_match='True'
                        break
                    else:
                        self.credit_match='False'
                        
            object_=checking_mata_data.validation()
            object_.cleanup()
            #import pdb;pdb.set_trace()
            writer1.writerow(object_.meta_data_validation(search_px_id__tmp))

        for aa in search_px_id__tmp:
           projectx_mapping_api=initialization_file.projectx_mapping_api_domain%aa
           data_resp_mapped=self.fetch_from_cloud_for_id(projectx_mapping_api)
           for yy in data_resp_mapped:
               if yy.get("data_source")=='Rovi' and yy.get("type")=='Program':
                   self.count_rovi=1+self.count_rovi
                   rovi_id.append(eval(yy.get("sourceId")))
                   rovi_mapping.append({str(aa):"Rovi_id:"+yy.get("sourceId")})
               if yy.get("data_source")==source and yy.get("type")=='Program' and yy.get('sub_type')=='MO':
                   self.count_source=1+self.count_source
                   source_mapping.append({str(aa):"id:"+yy.get("sourceId")})
               if yy.get("data_source")=='GuideBox' and yy.get("type")=='Program' and yy.get('sub_type')=='MO':
                   self.count_guidebox=1+self.count_guidebox
                   guidebox_mapping.append({str(aa):"Gb_id:"+yy.get("sourceId")})
        if self.count_rovi>1:
            
            preprod_api=initialization_file.preprod_beta_domain%'{}'.format(",".join([str(i) for i in rovi_id]))
            data_resp_preprod=self.fetch_from_cloud_for_id(preprod_api)
            variant_result=self.variant_parent_id_checking_rovi(data_resp_preprod,rovi_id)
            self.comment_variant_parent_id=variant_result[0]
            self.comment_variant_parent_id_present=variant_result[1]
                                          
            self.comment='Duplicate projectx ids found in search api and rovi has duplicate'
            writer.writerow([source,self.id,'','MO',self.link_details,self.movie_title,'','',self.release_year,self.link_expired,self.px_id,'','','',self.comment,self.comment,'',search_px_id__tmp,self.credit_match,self.count_rovi,self.count_guidebox,self.count_source,rovi_mapping,guidebox_mapping,source_mapping,self.comment_variant_parent_id_present,self.comment_variant_parent_id])
        else:    
            self.comment='Duplicate ids found in search api'
            writer.writerow([source,self.id,'','MO',self.link_details,self.movie_title,'','',self.release_year,self.link_expired,self.px_id,'','','',self.comment,self.comment,'',search_px_id__tmp,self.credit_match,self.count_rovi,self.count_guidebox,self.count_source,rovi_mapping,guidebox_mapping,source_mapping,self.comment_variant_parent_id_present,self.comment_variant_parent_id])


    def search_api_response_validation(self, data_resp_search, source, px_id, link_expired, writer, duplicate,writer1):
        search_px_id_tmp=[]
        search_px_id=[]
        search_px_id__tmp=[]
        search_px_id1=[]
        if duplicate=='False' or duplicate=='':
            for nn in data_resp_search.get('results'):
                #if nn.get("results"):
                for jj in nn.get("results"):
                    if jj.get("object").get("show_type")=='MO' or jj.get("object").get("show_type")=='OT':
                        search_px_id.append(jj.get("object").get("id"))
                        search_px_id1=search_px_id1+search_px_id  

                if search_px_id:
                    for mm in search_px_id:
                        if mm in self.px_id:
                            search_px_id_tmp.append(mm) 
                search_px_id=[]
                if len(search_px_id_tmp)==1 or search_px_id_tmp==[]:
                    search_px_id_tmp=[]  
                    duplicate='False'       
                elif search_px_id_tmp!=search_px_id__tmp:
                    search_px_id__tmp=search_px_id__tmp+search_px_id_tmp
                    duplicate='True'
                else:
                    duplicate='True'    
                        
            if search_px_id1:
                if len(search_px_id__tmp)>1 and duplicate=='True':
                    self.duplicate_results(search_px_id__tmp,source,writer,writer1)        
                else:
                    self.comment='Duplicate ids not found in search api'
                    #import pdb;pdb.set_trace()
                    projectx_api=initialization_file.projectx_preprod_domain%'{}'.format(",".join([str(i) for i in self.px_id]))
                    data_resp_projectx=self.fetch_from_cloud_for_id(projectx_api)
                    variant_result=self.variant_parent_id_checking_px(data_resp_projectx,self.px_id)
                    self.comment_variant_parent_id=variant_result[0]
                    self.comment_variant_parent_id_present=variant_result[1]
                    writer.writerow([source,self.id,'','MO',self.link_details,self.movie_title,'','',self.release_year,self.link_expired,self.px_id,'','','',self.comment,self.comment,'',search_px_id__tmp,'','','','','','','',self.comment_variant_parent_id_present,self.comment_variant_parent_id,'',''])            
            else:
                duplicate_api=initialization_file.duplicate_domain%(self.id,source,'MO')
                data_resp_duplicate=self.fetch_from_cloud_for_id(duplicate_api)
                if data_resp_duplicate:
                    duplicate='True'
                else:
                    duplicate='False'                                
                self.comment='Search api has no response'
                writer.writerow([source, self.id,'','MO', self.link_details, self.movie_title, '', '', 
                                 self.release_year, self.link_expired, self.px_id, '', '', '', 
                                 self.comment, self.comment, duplicate, 
                                 search_px_id__tmp, '', '', '', ''])   
        
    def search_api_call_response(self, px_id, source, link_expired, writer,writer1):
        #import pdb;pdb.set_trace() 
        next_page_url=""
        duplicate=""
        data_resp_search=dict()

        search_api=initialization_file.projectx_preprod_search_domain%urllib2.quote(self.movie_title)
        data_resp_search = self.fetch_from_cloud_for_id(search_api)
         
        while data_resp_search.get("results"):
            ## TODO: We are taking only the last response. We should take all  
            for nn in data_resp_search.get("results"):
                if nn.get("action_type")=="ott_search" and (nn.get("results")==[] or nn.get("results")):
                    next_page_url=nn.get("next_page_url")
                    if next_page_url is not None: 
                        search_api=initialization_file.domain_name+next_page_url.replace(' ',"%20")
                        data_resp_search = self.fetch_from_cloud_for_id(search_api)
                        self.search_api_response_validation(data_resp_search, source, px_id, link_expired, writer, duplicate,writer1)
                    else:    
                        data_resp_search={"results":[]}
                else:
                    data_resp_search={"results":[]}                           

    def extract_link_details(self, data):
        purchase_web_id=[]
        subscription_web_id=[]
        tv_everywhere_web_id=[]
        free_web_id=[]

        if data.get("purchase_web_sources"):
            purchase_web_id=getting_gb_link_id.getting_purchase_link_ids(data)                
        elif data.get("subscription_web_sources"):
            subscription_web_id=getting_gb_link_id.getting_link_subscription_ids(data)
        elif data.get("tv_everywhere_web_sources"):
            tv_everywhere_web_id=getting_gb_link_id.getting_link_tveverywhere_ids(data)
        elif data.get("free_web_sources"):    
            free_web_id=getting_gb_link_id.getting_link_free_web_ids(data)
        try:
           self.link_details=purchase_web_id+subscription_web_id+tv_everywhere_web_id+free_web_id
        except Exception:
           self.link_details=None    

    def duplicate_checking_movies(self, movie_data, source, writer,writer1): 
        #import pdb;pdb.set_trace()     
        print("\nFunction duplicate_checking_movies_called")
        self.id=movie_data.get("gb_id")
        self.movie_title = unidecode.unidecode(pinyin.get(movie_data.get("title")))
        #self.english_title = Translator().translate(movie_data.get("title"), dest='en')
        self.release_year = movie_data.get("release_year")
        self.extract_link_details(self.fetch_from_cloud_for_id(initialization_file.gb_details_api%(self.id, 'MO')))
  
        if self.link_details and self.link_details is not None:
            data_expired_api_resp = self.expiry_check(self.link_details)
            if data_expired_api_resp:
                self.link_expired = "False" if data_expired_api_resp.get("is_available")==False else "True"
            self.reverse_api_extract(self.link_details, source)
            print ("projectx_ids : {0}".format(self.px_id))
            if len(self.px_id) > 1:               
                self.search_api_call_response(self.px_id,source,self.link_expired,writer,writer1)
            else:
                self.comment=('No multiple ids for this link',self.link_details)
                self.result="No multiple ids for this link"
                writer.writerow([source, self.id,'','MO', self.link_details, self.movie_title, 
                                 '', '', self.release_year, self.link_expired, self.px_id, 
                                 '', '', '' ,self.comment,self.result, '', '', '', '', ''])    
        else:
            self.comment=('No link for this link')
            self.result="No link for this link"
            writer.writerow([source, self.id,'','MO', '', self.movie_title, 
                             '', '', self.release_year, '', self.px_id, 
                             '', '', '' ,self.comment,self.result, '', '', '', '', ''])    



"""Saayan"""

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
from fuzzywuzzy import fuzz
sys.setrecursionlimit(1500)
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file


class validation:

    def __init__(self):
        self.px_id1_alias=[]
        self.px_id1_alias_comment=''
        self.px_id2_alias=[]
        self.px_id2_alias_comment=''
        self.comment=''
        self.px_id2_credits_null='False'
        self.px_id1_credits_null='False'

        self.px_id1=0
        self.px_id1_show_type=''
        self.px_id1_variant_parent_id=0
        self.px_id1_is_group_language_primary=''
        self.px_id1_long_title=''
        self.px_id1_original_title=''
        self.px_id1_run_time=''
        self.px_id1_release_year=''
        self.px_id1_record_language=''
        self.px_id1_aliases=[]
        self.px_id1_credits=[]
        self.px_id1_db_credit_present='False'

        self.px_id2=0
        self.px_id2_show_type=''
        self.px_id2_variant_parent_id=0
        self.px_id2_is_group_language_primary=''
        self.px_id2_long_title=''
        self.px_id2_original_title=''
        self.px_id2_run_time=''
        self.px_id2_release_year=''
        self.px_id2_record_language=''
        self.px_id2_aliases=[]
        self.px_id2_credits=[]
        self.px_id2_db_credit_present='False'

        self.long_title_match=''
        self.original_title_match=''
        self.runtime_match=''
        self.release_year_match=''
        self.alias_title_match=''

    def cleanup(self):
        self.px_id1_alias=[]
        self.px_id1_alias_comment=''
        self.px_id2_alias=[]
        self.px_id2_alias_comment=''
        self.comment=''
        self.px_id2_credits_null='True'
        self.px_id1_credits_null='True'

        self.px_id1=0
        self.px_id1_show_type=''
        self.px_id1_variant_parent_id=0
        self.px_id1_is_group_language_primary=''
        self.px_id1_long_title=''
        self.px_id1_original_title=''
        self.px_id1_run_time=''
        self.px_id1_release_year=''
        self.px_id1_record_language=''
        self.px_id1_aliases=[]
        self.px_id1_credits=[]
        self.px_id1_db_credit_present='False'

        self.px_id2=0
        self.px_id2_show_type=''
        self.px_id2_variant_parent_id=0
        self.px_id2_is_group_language_primary=''
        self.px_id2_long_title=''
        self.px_id2_original_title=''
        self.px_id2_run_time=''
        self.px_id2_release_year=''
        self.px_id2_record_language=''
        self.px_id2_aliases=[]
        self.px_id2_credits=[]
        self.px_id2_db_credit_present='False'

        self.long_title_match=''
        self.original_title_match=''
        self.runtime_match=''
        self.release_year_match=''
        self.alias_title_match=''
        
    def credit_checked_in_db(self,api):
        resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':initialization_file.token}))
        data = resp.read()
        data_resp = json.loads(data)
        ## TODO: Validate response before returning
        return data_resp

    def fetch_from_cloud_for_id(self, api):
        resp = urllib2.urlopen(urllib2.Request(api,None,{'Authorization':initialization_file.token}))
        data = resp.read()
        data_resp = json.loads(data)
        ## TODO: Validate response before returning
        return data_resp    

    def checking_same_program(self,duplicate_id):
        #import pdb;pdb.set_trace()
        try:
            projectx_api_preprod=initialization_file.projectx_preprod_api%'{}'.format(",".join([str(i) for i in duplicate_id]))
            data_resp_ids=self.fetch_from_cloud_for_id(projectx_api_preprod)
            
            self.px_id1=data_resp_ids[0].get("id")
            self.px_id1_show_type=data_resp_ids[0].get("show_type").encode('utf-8')
            self.px_id1_variant_parent_id=data_resp_ids[0].get("variant_parent_id")
            self.px_id1_is_group_language_primary=data_resp_ids[0].get("is_group_language_primary")
            self.px_id1_long_title=unidecode.unidecode(data_resp_ids[0].get("long_title"))
            self.px_id1_original_title=unidecode.unidecode(data_resp_ids[0].get("original_title"))
            self.px_id1_run_time=data_resp_ids[0].get("run_time")
            self.px_id1_release_year=data_resp_ids[0].get("release_year")
            self.px_id1_record_language=data_resp_ids[0].get("record_language").encode('utf-8')
            self.px_id1_aliases=data_resp_ids[0].get("aliases")
            self.px_id1_credits=data_resp_ids[0].get("credits")

            if self.px_id1_credits:
                self.px_id1_credits_null='False'
            else:
                credit_db_api=initialization_file.credit_db_api%self.px_id1
                credit_resp_db=self.credit_checked_in_db(credit_db_api)
                if credit_resp_db:
                    self.px_id1_db_credit_present='True'        

            if self.px_id1_aliases!=[] and self.px_id1_aliases is not None:    
                for alias in self.px_id1_aliases:
                    if alias.get("source_name")=='GuideBox' and alias.get("language")=='ENG':
                        self.px_id1_alias.append(unidecode.unidecode(alias.get("alias")))
                    if alias.get("source_name")=='Rovi' and alias.get("type")=='alias_title' and alias.get("language")=='ENG':
                        self.px_id1_alias.append(unidecode.unidecode(alias.get("alias")))                
            else:
                self.px_id1_alias_comment='Null'    
            
            self.px_id2=data_resp_ids[1].get("id")
            self.px_id2_show_type=data_resp_ids[1].get("show_type").encode('utf-8')
            self.px_id2_variant_parent_id=data_resp_ids[1].get("variant_parent_id")
            self.px_id2_is_group_language_primary=data_resp_ids[1].get("is_group_language_primary")
            self.px_id2_long_title=unidecode.unidecode(data_resp_ids[1].get("long_title"))
            self.px_id2_original_title=unidecode.unidecode(data_resp_ids[1].get("original_title"))
            self.px_id2_run_time=data_resp_ids[1].get("run_time")
            self.px_id2_release_year=data_resp_ids[1].get("release_year")
            self.px_id2_record_language=data_resp_ids[1].get("record_language").encode('utf-8')
            self.px_id2_aliases=data_resp_ids[1].get("aliases")
            self.px_id2_credits=data_resp_ids[1].get("credits")

            if self.px_id2_credits:
                self.px_id2_credits_null='False'
            else:
                credit_db_api=initialization_file.credit_db_api%self.px_id2
                credit_resp_db=self.credit_checked_in_db(credit_db_api)
                if credit_resp_db:
                    self.px_id2_db_credit_present='True' 

            if self.px_id2_aliases!=[] and self.px_id2_aliases is not None:    
                for alias in self.px_id2_aliases:
                    if alias.get("source_name")=='GuideBox' and alias.get("language")=='ENG':
                        self.px_id2_alias.append(unidecode.unidecode(alias.get("alias")))            
                    if alias.get("source_name")=='Rovi' and alias.get("type")=='alias_title' and alias.get("language")=='ENG':
                        self.px_id2_alias.append(unidecode.unidecode(alias.get("alias")))    
            else:
                self.px_id2_alias_comment='Null' 
                
            if self.px_id1_long_title.upper() in self.px_id2_long_title.upper():
                self.long_title_match='True'
            else:
                if self.px_id2_long_title.upper() in self.px_id1_long_title.upper():
                    self.long_title_match='True'
                else:
                    ratio_title=fuzz.ratio(self.px_id1_long_title.upper(),self.px_id2_long_title.upper())
                    if ratio_title >=70:
                        self.long_title_match='True'
                    else:
                        self.long_title_match='False'    

            if self.px_id1_original_title.upper() in self.px_id2_original_title.upper():
                self.original_title_match='True'
            else:
                if self.px_id2_original_title.upper() in self.px_id1_original_title.upper():
                    self.original_title_match='True'
                else:
                    ratio_title=fuzz.ratio(self.px_id1_original_title.upper(),self.px_id2_original_title.upper())
                    if ratio_title >=70:
                        self.original_title_match='True'
                    else:
                        self.original_title_match='False'

            if self.px_id1_run_time==self.px_id2_run_time:
                self.runtime_match='True'
            else:
                self.runtime_match='False'

            if self.px_id1_release_year is not None and self.px_id2_release_year is not None:
                if self.px_id1_release_year==self.px_id2_release_year:
                    self.release_year_match='True'
                else:
                    r_y=self.px_id1_release_year
                    r_y=r_y+1
                    if r_y==self.px_id2_release_year:
                        self.release_year_match='True'
                    else:
                        r_y=r_y-2
                        if r_y==self.px_id2_release_year:
                            self.release_year_match='True'
                        else:
                            self.release_year_match='False'
            
            if self.px_id1_aliases and self.px_id2_aliases:
                for alias in self.px_id1_alias:
                    if alias in self.px_id2_long_title:
                        self.alias_title_match='True'
                        break
                    elif self.px_id2_long_title in alias:
                        self.alias_title_match='True'
                        break
                    elif self.px_id2_original_title in alias:
                        self.alias_title_match='True'
                        break
                    elif alias in self.px_id2_original_title:
                        self.alias_title_match='True'
                        break       
                    else:           
                        ratio_title=fuzz.ratio(self.px_id2_long_title.upper(),alias.upper())
                        if ratio_title >=70:
                            self.alias_title_match='True'
                            break
                        else:
                            ratio_title=fuzz.ratio(self.px_id2_original_title.upper(),alias.upper())
                            if ratio_title >=70:
                                self.alias_title_match='True'
                                break    
                            else:
                                self.alias_title_match='False'                
                                                    
            """if self.release_year_match=='True':
                self.comment='Match'
            elif self.alias_title_match=='True' and self.release_year_match=='True':
                self.comment='Match'
            elif (self.long_title_match=='True' or self.original_title_match=='True') and self.release_year_match=='True':
                self.comment='match'    
            else:
                self.comment='Different'"""                                     
                
            return [self.px_id1,self.px_id1_show_type,self.px_id1_variant_parent_id,self.px_id1_is_group_language_primary,self.px_id1_record_language,self.px_id2,self.px_id2_show_type,self.px_id2_variant_parent_id,self.px_id2_is_group_language_primary,self.px_id2_record_language,self.px_id1_credits_null,self.px_id1_db_credit_present,self.px_id2_credits_null,self.px_id2_db_credit_present,self.long_title_match,self.original_title_match,self.runtime_match,self.release_year_match,self.alias_title_match,self.comment]
             
        except Exception as e:
            print ("exception caught ......................................................",type(e),[self.px_id1,self.px_id2])
            print ("\n")
            print ("Retrying.............")
            print ("\n")    
            self.checking_same_program(duplicate_id)
    
    def meta_data_validation(self,duplicate_ids):
        print("Checking same program of duplicate cases..............")
        return self.checking_same_program(duplicate_ids)
                            
from fuzzywuzzy import fuzz
import os
import sys

"""
projectx_details={"px_credits":self.px_credits,"px_credit_present":self.px_credit_present,"px_long_title":self.px_long_title,"px_episode_title":self.px_episode_title,
                           "px_original_title":self.px_original_title,"px_description":self.px_description,"px_genres":self.px_genres,"px_aliases":self.px_aliases,
                            "px_release_year":self.px_release_year,"px_run_time":self.px_run_time,"px_season_number":self.px_season_number,"px_episode_number":self.px_episode_number,
                            "px_video_link_present":self.px_video_link_present,"px_images_details":self.px_images_details,"launch_id":self.launch_id}

source_details={"source_credits":self.hulu_credits,"source_credit_present":self.hulu_credit_present,"source_title":self.hulu_title,"source_description":self.hulu_description,
                    "source_genres":self.hulu_genres,"source_alternate_titles":self.hulu_alternate_titles,"source_release_year":self.hulu_release_year
                   ,"source_duration":self.hulu_duration,"source_season_number":self.hulu_season_number,"source_episode_number":self.hulu_episode_number,
                   "source_link_present":self.hulu_link_present,"source_images_details":self.hulu_images_details}                            
 """

class meta_data_validate:
    #initilization
    def __init__(self):
        self.title_match='False'
        self.description_match='False'
        self.genres_match='Null'
        self.release_year_match='False'
        self.season_number_match=''
        self.episode_number_match=''
        self.px_video_link_present=''
        self.source_link_present=''

    # meta_data_validation
    def meta_data_validation(self,_id,source_details,projectx_details,show_type):
        #import pdb;pdb.set_trace()
        if projectx_details:
            if show_type=='MO' or show_type=='SM':
                if projectx_details["px_original_title"]!='' or projectx_details["px_original_title"] is not None:
                    if projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                        self.title_match='True'
                    elif projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                        self.title_match='True'    
                    else:                
                        ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                        if ratio_title >=70:
                            self.title_match='True'
                        else:
                            ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                            if ratio_title >=70:
                                self.title_match='True'
                            else:
                                self.title_match='False'
                else:
                    if projectx_details["px_long_title"].upper() in source_details["source_title"].upper():
                        self.title_match='True'
                    elif source_details["px_long_title"].upper() in projectx_details["source_title"].upper():
                        self.title_match='True'
                    else:
                        ratio_title=fuzz.ratio(source_details["source_title"].upper(),projectx_details["px_long_title"].upper())
                        if ratio_title >=70:
                            self.title_match='True'
            else:
                if projectx_details["px_episode_title"].upper() in source_details["source_title"].upper():
                    self.title_match='True'
                elif source_details["source_title"].upper() in projectx_details["px_episode_title"].upper():
                    self.title_match='True'       
                else:                
                    ratio_title=fuzz.ratio(projectx_details["px_episode_title"].upper(),source_details["source_title"].upper())
                    if ratio_title >=70:
                        self.title_match='True'

            if projectx_details["px_description"]==source_details["source_description"]:
                self.description_match='True'
            try:
                if eval(source_details["source_release_year"]) == projectx_details["px_release_year"]:
                    self.release_year_match='True'
                elif projectx_details["px_release_year"]-1 == eval(source_details["source_release_year"]):
                    self.release_year_match='True'
                elif projectx_details["px_release_year"] ==  eval(source_details["source_release_year"])-1:
                    self.release_year_match='True'
            except Exception:
                if projectx_details["px_release_year"] == source_details["source_release_year"]:
                    self.release_year_match='True'        

            if show_type=='SE':
                if source_details["source_season_number"]==projectx_details["px_season_number"]:
                    self.season_number_match='True'
                else:
                    self.season_number_match='False'
                #import pdb;pdb.set_trace()
                if projectx_details["px_episode_number"]=="" and source_details[11]!="":
                    projectx_details["px_episode_number"]="0"    
                    if source_details["source_episode_number"]==projectx_details["px_episode_number"]:
                        self.episode_number_match='True'
                    else:
                        self.episode_number_match='False'
                else:
                     if source_details["source_episode_number"]==projectx_details["px_episode_number"]:
                         self.episode_number_match='True'
                     else:
                         self.episode_number_match='False'                   

            self.px_video_link_present=projectx_details["px_video_link_present"]
            self.source_link_present=source_details["source_link_present"]     

        return {"title_match":self.title_match,"description_match":self.description_match,"genres_match":self.genres_match,"release_year_match":self.release_year_match,
                "season_number_match":self.season_number_match,"episode_number_match":self.episode_number_match,"px_video_link_present":self.px_video_link_present,
                "source_link_present":self.source_link_present}

    



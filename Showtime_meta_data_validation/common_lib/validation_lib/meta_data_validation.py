from fuzzywuzzy import fuzz
import os
import sys

"""
source_details={"source_credits":vudu_credits,"source_credit_present":vudu_credit_present,"source_title":vudu_title,"source_description":vudu_description,
"source_genres":vudu_genres,"source_alternate_titles":vudu_alternate_titles,"source_release_year":vudu_release_year,"source_duration":vudu_duration,
"source_season_number":vudu_season_number,"source_episode_number":vudu_episode_number,"source_link_present":vudu_link_present,"source_images_details":vudu_images_details}

projectx_details={"px_credits":px_credits,"px_credit_present":px_credit_present,"px_long_title":px_long_title,"px_episode_title":px_episode_title,
"px_original_title":px_original_title,"px_description":px_description,"px_genres":px_genres,"px_aliases":px_aliases,"px_release_year":px_release_year,
"px_run_time":px_run_time,"px_season_number":px_season_number,"px_episode_number":px_episode_number,"px_video_link_present":px_video_link_present,
"px_images_details":px_images_details,"launch_id":launch_id}"""


def meta_data_validation(_id,source_details,projectx_details,show_type):
    #default:
    #import pdb;pdb.set_trace()
    title_match='False'
    description_match='False'
    release_year_match='False'
    duration_match='False'
    season_number_match=''
    episode_number_match=''
    px_video_link_present=''
    source_link_present=''

    if projectx_details:

        if show_type=='MO' or show_type=='SM':
            if projectx_details["px_original_title"]!='' or projectx_details["px_original_title"] is not None:
                if projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                    title_match='True'
                elif projectx_details["px_original_title"].upper() in source_details["source_title"].upper():
                    title_match='True'    
                else:                
                    ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                    if ratio_title >=70:
                        title_match='True'
                    else:
                        ratio_title=fuzz.ratio(projectx_details["px_original_title"].upper(),source_details["source_title"].upper())
                        if ratio_title >=70:
                            title_match='True'
                        else:
                            title_match='False'
            else:
                if projectx_details["px_long_title"].upper() in source_details["source_title"].upper():
                    title_match='True'
                elif source_details["source_title"].upper() in projectx_details["px_long_title"].upper():
                    title_match='True'
                else:
                    ratio_title=fuzz.ratio(source_details["source_title"].upper(),projectx_details["px_long_title"].upper())
                    if ratio_title >=70:
                        title_match='True'
        else:
            if projectx_details["px_episode_title"].upper() in source_details["source_title"].upper():
                title_match='True'
            elif source_details["source_title"].upper() in projectx_details["px_episode_title"].upper():
                title_match='True'       
            else:                
                ratio_title=fuzz.ratio(projectx_details["px_episode_title"].upper(),source_details["source_title"].upper())
                if ratio_title >=70:
                    title_match='True'

        if projectx_details["px_description"]==source_details["source_description"]:
            description_match='True'     

        try:
            if str(projectx_details["px_release_year"]) in str(source_details["source_release_year"]):
                release_year_match='True' 
            elif eval(source_details["source_release_year"]) == projectx_details["px_release_year"]:
                release_year_match='True'
            elif projectx_details["px_release_year"]-1 == eval(source_details["source_release_year"]):
                release_year_match='True'
            elif projectx_details["px_release_year"] ==  eval(source_details["source_release_year"])-1:
                release_year_match='True'
        except Exception:
            if source_details["source_release_year"]=="":
                source_details["source_release_year"]=0
                if projectx_details["px_release_year"] == source_details["source_release_year"]:
                    release_year_match='True'


        #import pdb;pdb.set_trace()
        if source_details["source_duration"] is not None or source_details["source_duration"]==0:
            if source_details["source_duration"]== projectx_details["px_run_time"]:
                duration_match='True'
            elif eval(source_details["source_duration"])== projectx_details["px_run_time"]:
                duration_match='True'
        else:
            duration_match='True'        
        #import pdb;pdb.set_trace()
        if show_type=='SE':
            if eval(source_details["source_season_number"])==projectx_details["px_season_number"]:
                season_number_match='True'
            else:
                season_number_match='False'

            #import pdb;pdb.set_trace()
            if (projectx_details["px_episode_number"]!="" or projectx_details["px_episode_number"] is not None) and source_details["source_episode_number"].encode()!='':
                try:     
                    if eval(source_details["source_episode_number"])==eval(projectx_details["px_episode_number"]):
                        episode_number_match='True'
                    else:
                        episode_number_match='False'            
                except Exception:
                    if eval(source_details["source_episode_number"])==projectx_details["px_episode_number"]:
                        episode_number_match='True'
                    else:
                        episode_number_match='False'


        px_video_link_present=projectx_details["px_video_link_present"]
        source_link_present=source_details["source_link_present"]     

    return {"title_match":title_match,"description_match":description_match,"release_year_match":release_year_match,
            "duration_match":duration_match,"season_number_match":season_number_match,"episode_number_match":episode_number_match,
            "px_video_link_present":px_video_link_present,"source_link_present":source_link_present}

    



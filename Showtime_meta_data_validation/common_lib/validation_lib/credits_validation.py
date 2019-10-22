"""Writer: Saayan"""


import os
import sys
import urllib2
import json
from fuzzywuzzy import fuzz

"""source_details={"source_credits":vudu_credits,"source_credit_present":vudu_credit_present,"source_title":vudu_title,"source_description":vudu_description,
"source_genres":vudu_genres,"source_alternate_titles":vudu_alternate_titles,"source_release_year":vudu_release_year,"source_duration":vudu_duration,
"source_season_number":vudu_season_number,"source_episode_number":vudu_episode_number,"source_link_present":vudu_link_present,"source_images_details":vudu_images_details}

projectx_details={"px_credits":px_credits,"px_credit_present":px_credit_present,"px_long_title":px_long_title,"px_episode_title":px_episode_title,
"px_original_title":px_original_title,"px_description":px_description,"px_genres":px_genres,"px_aliases":px_aliases,"px_release_year":px_release_year,
"px_run_time":px_run_time,"px_season_number":px_season_number,"px_episode_number":px_episode_number,"px_video_link_present":px_video_link_present,
"px_images_details":px_images_details,"launch_id":launch_id}"""


def credits_validation(source_details,projectx_details):
    #import pdb;pdb.set_trace()
    credit_match='True'#by default
    credit_mismatch=[]
    counter=0
    if projectx_details:
        if source_details["source_credit_present"]=='True' and projectx_details["px_credit_present"]=='True':
            for px_credits in projectx_details["px_credits"]:
                if px_credits in source_details["source_credits"]:
                    credit_match='True'
                    counter+=counter
                else:
                    if counter< len(projectx_details["px_credits"]):
                        for source_credits in source_details["source_credits"]:
                            credit_ratio=fuzz.ratio(px_credits.upper(),source_credits.upper())
                            if credit_ratio >=70:
                                credit_match='True'
                                counter+=counter
                                break    
                            else:
                                counter+=counter    
                    else:
                        credit_match='False'
                        credit_mismatch.append(px_credits)
                        
        if source_details["source_credit_present"]=='Null' and projectx_details["px_credit_present"]=='False':
            credit_match='True'
        if source_details["source_credit_present"]=='True' and projectx_details["px_credit_present"]=='False':
            credit_match='False'

    return (credit_match,credit_mismatch)                            

                






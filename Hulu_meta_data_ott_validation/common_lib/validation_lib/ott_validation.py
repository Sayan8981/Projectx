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

def ott_validation(projectx_details,source_id):

    comment_link='Null'   
    #import pdb;pdb.set_trace()
    try:
        if projectx_details["launch_id"]:
            if str(source_id) in projectx_details["launch_id"]:
                comment_link='Present'
            else:
                comment_link='Not_Present'
            return comment_link
        else:
            return comment_link
    except Exception:
        return comment_link             
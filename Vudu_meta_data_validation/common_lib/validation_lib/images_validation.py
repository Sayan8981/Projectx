import sys
import os


"""source_details={"source_credits":vudu_credits,"source_credit_present":vudu_credit_present,"source_title":vudu_title,"source_description":vudu_description,
"source_genres":vudu_genres,"source_alternate_titles":vudu_alternate_titles,"source_release_year":vudu_release_year,"source_duration":vudu_duration,
"source_season_number":vudu_season_number,"source_episode_number":vudu_episode_number,"source_link_present":vudu_link_present,"source_images_details":vudu_images_details}

projectx_details={"px_credits":px_credits,"px_credit_present":px_credit_present,"px_long_title":px_long_title,"px_episode_title":px_episode_title,
"px_original_title":px_original_title,"px_description":px_description,"px_genres":px_genres,"px_aliases":px_aliases,"px_release_year":px_release_year,
"px_run_time":px_run_time,"px_season_number":px_season_number,"px_episode_number":px_episode_number,"px_video_link_present":px_video_link_present,
"px_images_details":px_images_details,"launch_id":launch_id}"""


def images_validation(source_images,projectx_images):
    #import pdb;pdb.set_trace()
    image_url_match=''
    image_url_missing=''
    wrong_url=[]

    if projectx_images:
        source_images_url=source_images["source_images_details"]
        projectx_images=projectx_images["px_images_details"]

        if source_images_url!=[]:
            for images in projectx_images:
                if images in source_images_url:
                    image_url_match="True"
                    break 
                else:
                    image_url_missing="True" 
                    wrong_url.append(images.get("url")) 

    return (image_url_missing,wrong_url)     


    


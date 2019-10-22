import os
import sys

"""
source_details={"source_link_present":vudu_link_present,"source_id":vudu_id,"show_type":show_type}
projectx_details={"px_video_link_present":px_video_link_present,"launch_id":launch_id}
"""
#TODO: to validate the OTT links population in PX 
def ott_validation(projectx_details,source_id):

    comment_link='Null'
    
    #import pdb;pdb.set_trace()
    if projectx_details["launch_id"]!=[]:
        if source_id in projectx_details["launch_id"]:
            comment_link='Present'
        else:
            comment_link='Not_Present'
        return comment_link
    else:
        return comment_link         
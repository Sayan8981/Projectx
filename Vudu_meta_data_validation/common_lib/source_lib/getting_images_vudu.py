"""Writer: Saayan"""

import sys
import os
import urllib2
import json
import pymongo
import httplib
import socket     


def getting_images(source_id,show_type,source_show_id,details,db_table):
    #import pdb;pdb.set_trace()
    try:
        images_details=[]
        images_details.append({'url':details.get("image")})
            #source_images_url.append(image.get("image"))

        if show_type=='SE' and (images_details==[] or images_details):
            images_query=db_table.find({"show_type":'SM',"launch_id":source_show_id},{"image":1,"_id":0})
                    
            for images in images_query:
                images_details.append({'url':images.get("image")})
                #source_images_url.append(image.get("url"))

        return images_details

    except (Exception,RuntimeError):
        print ("exception caught .........................................................",source_id,show_type,source_show_id)    
        getting_images(source_id,show_type,source_show_id,details,db_table)
    
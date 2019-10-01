"""Writer: Saayan"""

import sys
import urllib2
import json
import os
from urllib2 import HTTPError
import httplib
import socket
     

def getting_images(source_id,show_type,source_show_id,details,db_table):
    #import pdb;pdb.set_trace()
    images_details=[]

    if details.get("thumbnails"):
        for image in details.get("thumbnails"):
            images_details.append({'url':image.get("url")})
    if show_type=='SM':
        images_details.append({'url':details.get("series").get("series_art")})
            
    return images_details

    
"""Writer: Saayan"""

import sys
import os


def px_images_details(px_show_id,data_images_px,show_type):
    #import pdb;pdb.set_trace()
    px_images_details=[]

    for images in data_images_px:
        px_images_details.append({'url':images.get("url")})
    if show_type=='SE' and px_images_details==[]:
        projectx_api=domain_url_lists.projectx_preprod_domain%px_show_id
        px_link=urllib2.Request(projectx_api)
        px_link.add_header('Authorization',domain_url_lists.token)
        px_resp=urllib2.urlopen(px_link)
        data_px=px_resp.read()
        data_px_images=json.loads(data_px)
        for images in data_px_images:
            px_images_details.append({'url':images.get("url")})
    return px_images_details 
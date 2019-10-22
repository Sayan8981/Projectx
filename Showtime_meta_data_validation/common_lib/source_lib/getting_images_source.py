"""Writer: Saayan"""

import sys
import os
import urllib2
import json
import pymysql
import httplib
import socket     

def mysql_connection_query(source_show_id):
    retry_count=0
    connection=pymysql.connect(user="root",passwd="branch@123",host="192.168.86.10",db="branch_service",port=3306)
    cur=connection.cursor()
    try:
        query="select img_url from showtime_programs where where source_program_id=source_show_id;"
        cur.execute(query)
        series_images=cur.fetchall()
        return series_images
    except (Exception,socket.error) as e:
        retry_count+=1
        if retry_count<=5:
            mysql_connection_query(source_show_id)        
    connection.close()


def getting_images(source_id,show_type,source_show_id,details,id_):
    #import pdb;pdb.set_trace()
    try:
        images_details=[]
        images_details.append({'url':(details[id_])[6]})
            #source_images_url.append(image.get("image"))

        if show_type=='SE' and (images_details==[] or images_details):
            images_query=mysql_connection_query(source_show_id)
                    
            for images in images_query:
                images_details.append({'url':images})
                #source_images_url.append(image.get("url"))

        return images_details

    except (Exception,RuntimeError):
        print ("exception caught .........................................................",source_id,show_type,source_show_id)    
        getting_images(source_id,show_type,source_show_id,details,id_)
    
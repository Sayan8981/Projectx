import pymongo
from pprint import pprint
import sys
import csv
import collections
from collections import Counter
import datetime
import urllib2
import json
import os
from urllib2 import HTTPError
import urllib
import socket
import httplib
from urllib2 import URLError
sys.setrecursionlimit(20000)
import unidecode
import threading
import re
import unidecode
from fuzzywuzzy import fuzz
from fuzzywuzzy import process  


 
def get_result_images(aa,name,writer1,writer,total):

    arr_rovi_px=[]
    arr_gb_px=[]
    preprod_images=[]
    projectx_images=[]
    format_match_=[]
    format_match=''
    format_mismatch_=[] 
    format_mismatch=''
    aspectRatio_match=''
    aspectRatio_match_=[]
    aspectRatio_mismatch_=[]
    aspectRatio_mismatch='' 
    orientation_match_=[]
    orientation_match=''
    orientation_mismatch_=[]
    orientation_mismatch=''
    imageType_match=''
    imageType_mismatch=''   
    imageType_match_=[]
    imageType_mismatch_=[]
    image_match=''
    images_url_source=[] 

    image_url_missing=''
    missing_url=[]
    projectx_series_id=''
    arr_gb_px_sm=[]
    arr_rovi_px_sm=[]

    preprod_images_flag=''
    projectx_images_flag=''
    projectx_response_flag=''
    preprod_response_flag=''
    Preprod_is_group_primary_language_flag=''
    preprod_videos_link_flag=''
    projectx_videos_link_flag=''
    Projectx_is_group_primary_language_flag=''  
    preprod_show_type=[]
    preprod_title=[]
    projectx_show_type=[]
    projectx_title=[]
    preprod_episode_title=[]
    projectx_episode_title=[]
    try:
        mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%aa
        response_link=urllib2.Request(mapping_api)
        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
        resp_link=urllib2.urlopen(response_link)
        data_link=resp_link.read()
        data_resp_link=json.loads(data_link)  
        if data_resp_link:
            for ii in data_resp_link:
                if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                    arr_rovi_px.append(ii.get("source_id"))
                if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                    arr_gb_px.append(ii.get("source_id"))    
            print({"Projectx ids":aa,"total id":total, "thread_name":name,"gb_id":arr_gb_px, "rovi_id":arr_rovi_px, "count rovi":len(arr_rovi_px),"count gb":len(arr_gb_px)})
            print("\n") 
            if (arr_rovi_px!=[] and len(arr_rovi_px)==1) or (arr_gb_px!=[] and len(arr_gb_px)==1):
                preprod_api="https://preprod.caavo.com/programs/%s?&ott=true"%'{}'.format(",".join([str(i) for i in arr_rovi_px]))
                response_link=urllib2.Request(preprod_api)
                response_link.add_header('Authorization','Caavo_Fyra_v1.0')
                resp_link=urllib2.urlopen(response_link)
                data_link=resp_link.read()
                data_resp_link1=json.loads(data_link)

                projectx_api="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%aa
                response_link=urllib2.Request(projectx_api)
                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                resp_link=urllib2.urlopen(response_link)
                data_link=resp_link.read()
                data_resp_link2=json.loads(data_link)
                if data_resp_link1:
                    preprod_response_flag='True'
                    for kk in data_resp_link1:
                        if kk.get("record_language")=='EN':
                            if kk.get("images")!=[]:
                                preprod_images_flag='True'
                                for i in kk.get("images"):
                                    preprod_images.append(i.get("url"))
                                preprod_show_type.append(kk.get("show_type"))
                                if kk.get("original_title")!='': 
                                    preprod_title.append(kk.get("original_title"))
                                else:
                                    preprod_title.append(kk.get("long_title"))  
                                if kk.get("original_episode_title")!='':
                                    preprod_episode_title.append(kk.get("original_episode_title"))
                                else:
                                    preprod_episode_title.append(kk.get("episode_title")) 
                                preprod_release_year=kk.get("release_year")
                                preprod_episode_number=kk.get("episode_season_sequence")
                                preprod_season_number=kk.get("episode_season_number")
                                is_group_primary_language=kk.get("is_group_language_primary")
                                if is_group_primary_language==True:
                                    Preprod_is_group_primary_language_flag="True"
                                elif is_group_primary_language is None:
                                    projectx_is_group_primary_language_flag="Null"
                                else:
                                    preprod_is_group_primary_language_flag="False"
                                if kk.get("videos")!=[]:
                                    preprod_videos_link_flag="True"
                                else:
                                    preprod_videos_link_flag="False"  
                            
                            else:
                                preprod_show_type.append(kk.get("show_type"))
                                if kk.get("original_title")!='':
                                    preprod_title.append(kk.get("original_title"))
                                else:
                                    preprod_title.append(kk.get("long_title"))
                                if kk.get("original_episode_title")!='':
                                    preprod_episode_title.append(kk.get("original_episode_title"))
                                else:
                                    preprod_episode_title.append(kk.get("episode_title"))
                                preprod_images_flag='False'
                                preprod_release_year=kk.get("release_year")
                                preprod_episode_number=kk.get("episode_season_sequence")
                                preprod_season_number=kk.get("episode_season_number")
                                is_group_primary_language=kk.get("is_group_language_primary")
                                if is_group_primary_language==True:
                                    Preprod_is_group_primary_language_flag="True"
                                elif is_group_primary_language is None:
                                    preprod_is_group_primary_language_flag="Null"
                                else:
                                    preprod_is_group_primary_language_flag="False"
                                if kk.get("videos")!=[]:
                                    preprod_videos_link_flag="True"
                                else:
                                    preprod_videos_link_flag="False"
                else:
                    preprod_response_flag='False'
                if data_resp_link2:
                    projectx_response_flag='True'   
                    for jj in data_resp_link2:
                        if jj.get("record_language")=="EN":
                            if jj.get("images")!=[]:
                                projectx_images_flag='True'
                                for i in jj.get("images"):
                            
                                    projectx_images.append({"url":i.get("url"),"format":i.get("format"),"aspect_ratio":i.get("aspect_ratio"),"orientation":i.get("orientation"),"image_type":i.get("image_type")})
                                projectx_show_type.append(jj.get("show_type"))
                                if projectx_show_type[0].encode()=='OT':
                                    projectx_show_type[0]='MO' 
                                projectx_series_id=jj.get("series_id")
                                print ("projectx_series_id: ", projectx_series_id)
                                if jj.get("original_title")!='':
                                    projectx_title.append(jj.get("original_title"))
                                else:
                                    projectx_title.append(jj.get("long_title"))    
                                if jj.get("original_episode_title")!='':
                                    projectx_episode_title.append(jj.get("original_episode_title"))
                                else:
                                    projectx_episode_title.append(jj.get("episode_title"))
                                projectx_release_year=jj.get("release_year")
                                projectx_episode_number=jj.get("episode_season_sequence")
                                projectx_season_number=jj.get("episode_season_number")
                                is_group_primary_language=jj.get("is_group_language_primary")
                                if is_group_primary_language==True:
                                    Projectx_is_group_primary_language_flag="True"
                                elif is_group_primary_language is None:
                                    projectx_is_group_primary_language_flag="Null" 
                                else:
                                    projectx_is_group_primary_language_flag="False"
                                if jj.get("videos")!=[]:
                                    projectx_videos_link_flag="True"
                                else:
                                    projectx_videos_link_flag="False"
                            else:
                                projectx_show_type.append(jj.get("show_type"))
                                if projectx_show_type[0].encode()=='OT':
                                    projectx_show_type[0]='MO'
                                projectx_series_id=jj.get("series_id")
                                if jj.get("original_title")!='':
                                    projectx_title.append(jj.get("original_title"))
                                else:
                                    projectx_title.append(jj.get("long_title"))
                                if jj.get("original_episode_title")!='':
                                    projectx_episode_title.append(jj.get("original_episode_title"))
                                else:
                                    projectx_episode_title.append(jj.get("episode_title"))  
                                projectx_images_flag='False'
                                projectx_release_year=jj.get("release_year")
                                projectx_episode_number=jj.get("episode_season_sequence")
                                projectx_season_number=jj.get("episode_season_number")
                                is_group_primary_language=jj.get("is_group_language_primary")
                                if is_group_primary_language==True:
                                    Projectx_is_group_primary_language_flag="True"
                                elif is_group_primary_language is None:
                                    projectx_is_group_primary_language_flag="Null"
                                else:
                                    projectx_is_group_primary_language_flag="False"
                                if jj.get("videos")!=[]:
                                    projectx_videos_link_flag="True"
                                else:
                                    projectx_videos_link_flag="False" 
                else: 
                    projectx_response_flag='False'
                if preprod_response_flag=='True' and projectx_response_flag=='True' and preprod_videos_link_flag=='True' and projectx_videos_link_flag=='True': #  """*************"""
                    if projectx_images!=[] and preprod_images!=[]:
                        if arr_rovi_px!=[] and arr_gb_px==[]:
                            
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            for nn in data_resp_link3:
                                images_url_source.append(nn.get("imageUrl"))

                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    for nn in data_resp_link3:
                                        images_url_source.append(nn.get("imageUrl"))
                                    if arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1:
                                        gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                        response_link=urllib2.Request(gb_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link4=json.loads(data_link)
                                        for nn in data_resp_link4:
                                            images_url_source.append(nn.get("url")) 
      

                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link3:
                                        if mm.get("url")==nn.get("imageUrl"):    
                                            if mm.get("format")==nn.get("format"):
                                                format_match="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch='True'  
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspectRatio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch='True' 
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspectRatio")})
                                            if mm.get("orientation") is not None:
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")}) 
                                            if mm.get("image_type")==nn.get("imageType"):
                                                imageType_match='True' 
                                                imageType_match_.append({"True":1})
                                            else:
                                                imageType_mismatch='True'
                                                imageType_mismatch_.append({"url":mm.get("url"),"wrong_imageType":mm.get("image_type"),"Expected":nn.get("imageType")})
                                                
                                else:
                                    image_url_missing="True" 
                                    missing_url.append(mm.get("url"))               
                            if format_mismatch=='True' or aspectRatio_mismatch=='True' or orientation_mismatch=='True' or imageType_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,preprod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,"Fail"])
                            else:
                                image_match="True"
                                 
                        if arr_rovi_px==[] and arr_gb_px!=[]:
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link=urllib2.Request(gb_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link4=json.loads(data_link)
                            for nn in data_resp_link4:
                                images_url_source.append(nn.get("url"))

                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                    response_link=urllib2.Request(gb_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link4=json.loads(data_link)
                                    for nn in data_resp_link4:
                                        images_url_source.append(nn.get("url"))   
                                    if arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1:
                                        rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                        response_link=urllib2.Request(rovi_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link3=json.loads(data_link)
                                        for nn in data_resp_link3:
                                            images_url_source.append(nn.get("imageUrl")) 



                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link4:
                                        if mm.get("url")==nn.get("url"):
                                            if mm.get("format")==nn.get("format"):
                                                format_match_="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch_='True'
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspect_ratio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch_='True'
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspect_ratio")})
                                            if mm.get("orientation") is not None:
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")})  
                                else:
                                    image_url_missing="True"
                                    missing_url.append(mm.get("url"))
                            if format_mismatch=='True' or aspectRatio_mismatch=="True" or orientation_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,preprod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_match_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,"Fail"])
                            else:
                                image_match="True"         


                        if arr_rovi_px!=[] and arr_gb_px!=[]:
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            for nn in data_resp_link3:
                                images_url_source.append(nn.get("imageUrl"))
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link=urllib2.Request(gb_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link4=json.loads(data_link)
                            for nn in data_resp_link4:
                                images_url_source.append(nn.get("url"))

                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    for nn in data_resp_link3:
                                        images_url_source.append(nn.get("imageUrl"))

                                    gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                    response_link=urllib2.Request(gb_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link4=json.loads(data_link)
                                    for nn in data_resp_link4:
                                        images_url_source.append(nn.get("url"))

                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link4:
                                        if mm.get("url")==nn.get("url"):
                                            if mm.get("format")==nn.get("format"):
                                                format_match="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch='True'
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspect_ratio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch='True'
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspect_ratio")})
                                            if mm.get("orientation") is not None:
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")}) 
                                        else:
                                            for kk in data_resp_link3:
                                                if mm.get("url")==kk.get("imageUrl"):
                                                    if mm.get("format")==kk.get("format"):
                                                        format_match="True"
                                                        format_match_.append({"True":1})
                                                    else:
                                                        format_mismatch='True'
                                                        format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":kk.get("format")})
                                                    if mm.get("aspect_ratio")==kk.get("aspectRatio"):
                                                        aspectRatio_match='True'
                                                        aspectRatio_match_.append({"True":1})
                                                    else:
                                                        aspectRatio_mismatch='True'
                                                        aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":kk.get("aspectRatio")})
                                                    if mm.get("orientation") is not None:
                                                        if mm.get("orientation").lower()==kk.get("orientation").lower():
                                                            orientation_match='True'
                                                            orientation_match_.append({'True':1})
                                                        else:
                                                            orientation_mismatch='True'
                                                            orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":kk.get("orientation")})
                                                    else:
                                                        if mm.get("orientation")==kk.get("orientation"):
                                                            orientation_match='True'
                                                            orientation_match_.append({'True':1})
                                                        else:
                                                            orientation_mismatch='True'
                                                            orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":kk.get("orientation")})

                                                    if mm.get("image_type")==kk.get("imageType"):
                                                        imageType_match='True'
                                                        imageType_match_.append({"True":1})
                                                    else:
                                                        imageType_mismatch='True'
                                                        imageType_mismatch_.append({"url":mm.get("url"),"wrong_imageType":mm.get("image_type"),"Expected":kk.get("imageType")})
                                              
                                else:
                                    image_url_missing="True"
                                    missing_url.append(mm.get("url"))  

                            if format_mismatch=='True' or aspectRatio_mismatch=="True" or orientation_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,preprod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,"Fail"])
                            else:
                                image_match="True" 

        
                    if projectx_images!=[] and preprod_images==[]:
                        if arr_rovi_px!=[] and arr_gb_px==[]:
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            for nn in data_resp_link3:
                                images_url_source.append(nn.get("imageUrl"))
                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    for nn in data_resp_link3:
                                        images_url_source.append(nn.get("imageUrl"))
                                    if arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1:
                                        gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                        response_link=urllib2.Request(gb_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link4=json.loads(data_link)
                                        for nn in data_resp_link4:
                                            images_url_source.append(nn.get("url"))

                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link3:
                                        if mm.get("url")==nn.get("imageUrl"):    
                                            if mm.get("format")==nn.get("format"):
                                                format_match="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch='True'  
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspectRatio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch='True' 
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspectRatio")})
                                            if mm.get("orientation") is not None:
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")})
                                            if mm.get("image_type")==nn.get("imageType"):
                                                imageType_match='True' 
                                                imageType_match_.append({"True":1})
                                            else:
                                                imageType_mismatch='True'
                                                imageType_mismatch_.append({"url":mm.get("url"),"wrong_imageType":mm.get("image_type"),"Expected":nn.get("imageType")})
                                                
                                else:
                                    image_url_missing="True" 
                                    missing_url.append(mm.get("url"))               
                            if format_mismatch=='True' or aspectRatio_mismatch=="True" or orientation_mismatch=='True' or imageType_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,preprod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch,"Fail"])
                            else:
                                image_match="True"
                                 
                        if arr_rovi_px==[] and arr_gb_px!=[]:
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link=urllib2.Request(gb_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link4=json.loads(data_link)
                            for nn in data_resp_link4:
                                images_url_source.append(nn.get("url"))
                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                    response_link=urllib2.Request(gb_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link4=json.loads(data_link)
                                    for nn in data_resp_link4:
                                        images_url_source.append(nn.get("url"))
                                    if arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1:
                                        rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                        response_link=urllib2.Request(rovi_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link3=json.loads(data_link)
                                        for nn in data_resp_link3:
                                            images_url_source.append(nn.get("imageUrl"))

                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link4:
                                        if mm.get("url")==nn.get("url"):
                                            if mm.get("format")==nn.get("format"):
                                                format_match="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch='True'
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspect_ratio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch='True'
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspect_ratio")})
                                            if mm.get("orientation") is not None:
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")})
                                else:
                                    image_url_missing="True"
                                    missing_url.append(mm.get("url"))
                            if format_mismatch=='True' or aspectRatio_mismatch=="True" or orientation_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,preprod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,"Fail"])
                            else:
                                image_match="True"         


                        if arr_rovi_px!=[] and arr_gb_px!=[]:
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            for nn in data_resp_link3:
                                images_url_source.append(nn.get("imageUrl"))
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link=urllib2.Request(gb_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link4=json.loads(data_link)
                            for nn in data_resp_link4:
                                images_url_source.append(nn.get("url"))

                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    for nn in data_resp_link3:
                                        images_url_source.append(nn.get("imageUrl"))

                                    gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                    response_link=urllib2.Request(gb_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link4=json.loads(data_link)
                                    for nn in data_resp_link4:
                                        images_url_source.append(nn.get("url"))
  
                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link4:
                                        if mm.get("url")==nn.get("url"):
                                            if mm.get("format")==nn.get("format"):
                                                format_match="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch='True'
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspect_ratio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch='True'
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspect_ratio")})
                                            if mm.get("orientation") is not None:
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")})
                                              
                                        else:
                                            for kk in data_resp_link3:
                                                if mm.get("url")==kk.get("imageUrl"):
                                                    if mm.get("format")==kk.get("format"):
                                                        format_match="True"
                                                        format_match_.append({"True":1})
                                                    else:
                                                        format_mismatch='True'
                                                        format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":kk.get("format")})
                                                    if mm.get("aspect_ratio")==kk.get("aspectRatio"):
                                                        aspectRatio_match='True'
                                                        aspectRatio_match_.append({"True":1})
                                                    else:
                                                        aspectRatio_mismatch='True'
                                                        aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":kk.get("aspectRatio")})
                                                    if mm.get("orientation") is not None:
                                                        if mm.get("orientation").lower()==kk.get("orientation").lower():
                                                            orientation_match='True'
                                                            orientation_match_.append({'True':1})
                                                        else:
                                                            orientation_mismatch='True'
                                                            orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":kk.get("orientation").lower()})
                                                    else:
                                                        if mm.get("orientation")==kk.get("orientation"):
                                                            orientation_match='True'
                                                            orientation_match_.append({'True':1})
                                                        else:
                                                            orientation_mismatch='True'
                                                            orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":kk.get("orientation")}) 

                                                    if mm.get("image_type")==kk.get("imageType"):
                                                        imageType_match='True'
                                                        imageType_match_.append({"True":1})
                                                    else:
                                                        imageType_mismatch='True'
                                                        imageType_mismatch_.append({"url":mm.get("url"),"wrong_imageType":mm.get("image_type"),"Expected":kk.get("imageType")})
                                              
                                else:
                                    image_url_missing="True"
                                    missing_url.append(mm.get("url"))   
                            if format_mismatch=='True' or aspectRatio_mismatch=="True" or orientation_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,preprod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,"Fail"])
                            else:
                                image_match="True"    

                    if projectx_images==[] and preprod_images!=[]:
                       
                        if arr_gb_px!=[] and len(arr_gb_px)==1:
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link_gb=urllib2.Request(gb_images_api)
                            response_link_gb.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link_gb=urllib2.urlopen(response_link_gb)
                            data_link_gb=resp_link_gb.read()
                            data_resp_link4=json.loads(data_link_gb)
                    #        import pdb;pdb.set_trace()
                            if data_resp_link4!=[]:
                                writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,preprod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but gb id have'])
                            else:
                                if projectx_show_type[0].encode()=='SE':
                                    mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                    response_link=urllib2.Request(mapping_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link=json.loads(data_link)
                                    if data_resp_link:
                                        for ii in data_resp_link:
                                            if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                arr_rovi_px_sm.append(ii.get("source_id"))
                                            if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                arr_gb_px_sm.append(ii.get("source_id"))
                                    if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                        gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                        response_link_gb=urllib2.Request(gb_images_api)
                                        response_link_gb.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link_gb=urllib2.urlopen(response_link_gb)
                                        data_link_gb=resp_link_gb.read()
                                        data_resp_link4=json.loads(data_link_gb)
                                        if data_resp_link4!=[]:
                                            writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,preprod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but gb id SM have'])
                                        else:
                                            pass  
                                else:
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    if data_resp_link3!=[]:
                                        writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id have'])   
                                    else:
                                        if projectx_show_type[0].encode()=='SE':
                                            mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                            response_link=urllib2.Request(mapping_api)
                                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                            resp_link=urllib2.urlopen(response_link)
                                            data_link=resp_link.read()
                                            data_resp_link=json.loads(data_link)
                                            if data_resp_link:
                                                for ii in data_resp_link:
                                                    if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                        arr_rovi_px_sm.append(ii.get("source_id"))
                                                    if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                        arr_gb_px_sm.append(ii.get("source_id"))
                                            if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                                rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                                response_link=urllib2.Request(rovi_images_api)
                                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                                resp_link=urllib2.urlopen(response_link)
                                                data_link=resp_link.read()
                                                data_resp_link3=json.loads(data_link)
                                                if data_resp_link3!=[]:
                                                    writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id SM have']) 
                                                else:
                                                    pass

                        else:
                   #         import pdb;pdb.set_trace()
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            if data_resp_link3!=[]:  
                                writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id have'])                        
                            else:
                                if projectx_show_type[0].encode()=='SE':
                                    mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                    response_link=urllib2.Request(mapping_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link=json.loads(data_link)
                                    if data_resp_link:
                                        for ii in data_resp_link:
                                            if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                arr_rovi_px_sm.append(ii.get("source_id"))
                                            if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                arr_gb_px_sm.append(ii.get("source_id"))
                                    if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                        rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                        response_link=urllib2.Request(rovi_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link3=json.loads(data_link)
                                        if data_resp_link3!=[]:
                                            writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id have'])
                                        else: 
                                            pass  
                                else:
                                    pass   
                            
                    if projectx_images==[] and preprod_images==[]:
                        if arr_gb_px!=[] and len(arr_gb_px)==1:
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link_gb=urllib2.Request(gb_images_api)
                            response_link_gb.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link_gb=urllib2.urlopen(response_link_gb)
                            data_link_gb=resp_link_gb.read()
                            data_resp_link4=json.loads(data_link_gb)
                    #        import pdb;pdb.set_trace()
                            if data_resp_link4!=[]:
                                writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but gb id have'])
                            else:
                                if projectx_show_type[0].encode()=='SE':
                                    mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                    response_link=urllib2.Request(mapping_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link=json.loads(data_link) 
                                    if data_resp_link:
                                        for ii in data_resp_link:
                                            if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                arr_rovi_px_sm.append(ii.get("source_id"))
                                            if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                arr_gb_px_sm.append(ii.get("source_id"))
                                    if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1): 
                                        gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                        response_link_gb=urllib2.Request(gb_images_api)
                                        response_link_gb.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link_gb=urllib2.urlopen(response_link_gb)
                                        data_link_gb=resp_link_gb.read()
                                        data_resp_link4=json.loads(data_link_gb)
                                        if data_resp_link4!=[]:
                                            writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but gb id SM have'])
                                        else:
                                            pass
                                else: 
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    if data_resp_link3!=[]:
                                        writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id have'])           
                                    else:
                                        if projectx_show_type[0].encode()=='SE':
                                            if data_resp_link:
                                                for ii in data_resp_link:
                                                    if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                        arr_rovi_px_sm.append(ii.get("source_id"))
                                                    if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                        arr_gb_px_sm.append(ii.get("source_id"))
                                            if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                                rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                                response_link=urllib2.Request(rovi_images_api)
                                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                                resp_link=urllib2.urlopen(response_link)
                                                data_link=resp_link.read()
                                                data_resp_link3=json.loads(data_link)
                                                if data_resp_link3!=[]:
                                                    writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id SM have'])
                                                else:
                                                    pass
                        else:
                   #         import pdb;pdb.set_trace()
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            if data_resp_link3!=[]:
                                writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id have'])               
                            else:
                                if projectx_show_type[0].encode()=='SE':
                                    mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                    response_link=urllib2.Request(mapping_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link=json.loads(data_link)
                                    if data_resp_link:
                                        for ii in data_resp_link:
                                            if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                arr_rovi_px_sm.append(ii.get("source_id"))
                                            if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                arr_gb_px_sm.append(ii.get("source_id"))
                                    if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                        rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                        response_link=urllib2.Request(rovi_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link3=json.loads(data_link)
                                        if data_resp_link3!=[]:
                                            writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id SM have'])
                                        else:
                                            pass

                           
                if preprod_response_flag=='True' and projectx_response_flag=='False' and preprod_videos_link_flag=='True': #"""*******************"""
                    if (arr_rovi_px!=[] and arr_gb_px!=[]) or (arr_rovi_px!=[] and arr_gb_px==[]) or (arr_rovi_px==[] and arr_gb_px!=[]):
                    
                        writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'projectx id have no response']) 


                if preprod_response_flag=='False' and projectx_response_flag=='True' and projectx_videos_link_flag=='True':          # """********************"""
        
                    if projectx_images!=[] and preprod_images==[]:
                        if arr_rovi_px!=[] and arr_gb_px==[]:
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            for nn in data_resp_link3:
                                images_url_source.append(nn.get("imageUrl"))

                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    for nn in data_resp_link3:
                                        images_url_source.append(nn.get("imageUrl")) 
                                    if arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1:
                                        gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                        response_link=urllib2.Request(gb_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link4=json.loads(data_link)
                                        for nn in data_resp_link4:
                                            images_url_source.append(nn.get("url"))

                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link3:
                                        if mm.get("url")==nn.get("imageUrl"):    
                                            if mm.get("format")==nn.get("format"):
                                                format_match="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch='True'  
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspectRatio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch='True' 
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspectRatio")})
                                            if mm.get("orientation") is not None:
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")})  
                                            if mm.get("image_type")==nn.get("imageType"):
                                                imageType_match='True' 
                                                imageType_match_.append({"True":1})
                                            else:
                                                imageType_mismatch='True'
                                                imageType_mismatch_.append({"url":mm.get("url"),"wrong_imageType":mm.get("image_type"),"Expected":nn.get("imageType")})
                                                
                                else:
                                    image_url_missing="True" 
                                    missing_url.append(mm.get("url"))               
                            if format_mismatch=='True' or aspectRatio_mismatch=="True" or orientation_mismatch=='True' or imageType_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,"Fail"])
                            else:
                                image_match="True"
                                 
                        if arr_rovi_px==[] and arr_gb_px!=[]:
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link=urllib2.Request(gb_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link4=json.loads(data_link)
                            for nn in data_resp_link4:
                                images_url_source.append(nn.get("url"))

                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                    response_link=urllib2.Request(gb_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link4=json.loads(data_link)
                                    for nn in data_resp_link4:
                                        images_url_source.append(nn.get("url"))
                                    if arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1:
                                        rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                        response_link=urllib2.Request(rovi_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link3=json.loads(data_link)
                                        for nn in data_resp_link3:
                                            images_url_source.append(nn.get("imageUrl"))

 
                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link4:
                                        if mm.get("url")==nn.get("url"):
                                            if mm.get("format")==nn.get("format"):
                                                format_match="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch='True'
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspect_ratio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch='True'
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspect_ratio")})
                                            if mm.get("orientation") is not None:
         
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")})
                                else:
                                    image_url_missing="True"
                                    missing_url.append(mm.get("url"))
                            if format_mismatch=='True' or aspectRatio_mismatch=="True" or orientation_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,"Fail"])
                            else:
                                image_match="True"         


                        if arr_rovi_px!=[] and arr_gb_px!=[]:
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            for nn in data_resp_link3:
                                images_url_source.append(nn.get("imageUrl"))
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link=urllib2.Request(gb_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link4=json.loads(data_link)
                            for nn in data_resp_link4:
                                images_url_source.append(nn.get("url"))

                            if images_url_source==[] and projectx_show_type[0].encode()=='SE':
                                mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                response_link=urllib2.Request(mapping_api)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link=json.loads(data_link)
                                if data_resp_link:
                                    for ii in data_resp_link:
                                        if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                            arr_rovi_px_sm.append(ii.get("source_id"))
                                        if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                            arr_gb_px_sm.append(ii.get("source_id"))
                                if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    for nn in data_resp_link3:
                                        images_url_source.append(nn.get("imageUrl"))

                                    gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                    response_link=urllib2.Request(gb_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link4=json.loads(data_link)
                                    for nn in data_resp_link4:
                                        images_url_source.append(nn.get("url"))

                            for mm in projectx_images:
                                if mm.get("url") in images_url_source:
                                    image_url_match="True"
                                    for nn in data_resp_link4:
                                        if mm.get("url")==nn.get("url"):
                                            if mm.get("format")==nn.get("format"):
                                                format_match="True"
                                                format_match_.append({"True":1})
                                            else:
                                                format_mismatch='True'
                                                format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":nn.get("format")})
                                            if mm.get("aspect_ratio")==nn.get("aspect_ratio"):
                                                aspectRatio_match='True'
                                                aspectRatio_match_.append({"True":1})
                                            else:
                                                aspectRatio_mismatch='True'
                                                aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":nn.get("aspect_ratio")})
                                            if mm.get("orientation") is not None : 
                                                if mm.get("orientation").lower()==nn.get("orientation").lower():
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":nn.get("orientation").lower()})
                                            else:
                                                if mm.get("orientation")==nn.get("orientation"):
                                                    orientation_match='True'
                                                    orientation_match_.append({'True':1})
                                                else:
                                                    orientation_mismatch='True'
                                                    orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":nn.get("orientation")})   
                                        else:
                                            for kk in data_resp_link3:
                                                if mm.get("url")==kk.get("imageUrl"):
                                                    if mm.get("format")==kk.get("format"): 
                                                        format_match="True"
                                                        format_match_.append({"True":1})
                                                    else:
                                                        format_mismatch='True'
                                                        format_mismatch_.append({"url":mm.get("url"),"wrong_format":mm.get("format"),"Expected":kk.get("format")})
                                                    if mm.get("aspect_ratio")==kk.get("aspectRatio"):
                                                        aspectRatio_match='True'
                                                        aspectRatio_match_.append({"True":1})
                                                    else:
                                                        aspectRatio_mismatch='True'
                                                        aspectRatio_mismatch_.append({"url":mm.get("url"),"wrong_aspectRatio":mm.get("aspect_ratio"),"Expected":kk.get("aspectRatio")})
                                                    if mm.get("orientation") is not None:
                                                        if mm.get("orientation").lower()==kk.get("orientation").lower():
                                                            orientation_match='True'
                                                            orientation_match_.append({'True':1})
                                                        else:
                                                            orientation_mismatch='True'
                                                            orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation").lower(),"Expected":kk.get("orientation").lower()})
                                                    else:
                                                        if mm.get("orientation")==kk.get("orientation"):
                                                            orientation_match='True'
                                                            orientation_match_.append({'True':1})
                                                        else:
                                                            orientation_mismatch='True'
                                                            orientation_mismatch_.append({"url":mm.get("url"),'wrong_orientation':mm.get("orientation"),"Expected":kk.get("orientation")})
                                                    if mm.get("image_type")==kk.get("imageType"):
                                                        imageType_match='True'
                                                        imageType_match_.append({"True":1})
                                                    else:
                                                        imageType_mismatch='True'
                                                        imageType_mismatch_.append({"url":mm.get("url"),"wrong_imageType":mm.get("image_type"),"Expected":kk.get("imageType")})
                                              
                                else:
                                    image_url_missing="True"
                                    missing_url.append(mm.get("url"))   
                            if format_mismatch=='True' or aspectRatio_mismatch=="True" or orientation_mismatch=='True' or image_url_missing=="True":
                                image_match="False"
                                writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,"Fail"])
                            else:
                                image_match="True"    

                    if projectx_images==[] and preprod_images==[]:
                        if arr_gb_px!=[] and len(arr_gb_px)==1:
                            gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px[0].encode(),projectx_show_type[0].encode())
                            response_link_gb=urllib2.Request(gb_images_api)
                            response_link_gb.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link_gb=urllib2.urlopen(response_link_gb)
                            data_link_gb=resp_link_gb.read()
                            data_resp_link4=json.loads(data_link_gb)
                    #        import pdb;pdb.set_trace()
                            if data_resp_link4!=[]:
                                writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but gb id have'])
                            else:
                                if projectx_show_type[0].encode()=='SE':
                                    mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                    response_link=urllib2.Request(mapping_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link=json.loads(data_link)  
                                    if data_resp_link:
                                        for ii in data_resp_link:
                                            if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                arr_rovi_px_sm.append(ii.get("source_id"))
                                            if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                arr_gb_px_sm.append(ii.get("source_id"))
                                    if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                        gb_images_api="http://34.231.212.186:81/projectx/%s/guideboximage?showType=%s"%(arr_gb_px_sm[0].encode(),'SM')
                                        response_link_gb=urllib2.Request(gb_images_api)
                                        response_link_gb.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link_gb=urllib2.urlopen(response_link_gb)
                                        data_link_gb=resp_link_gb.read()
                                        data_resp_link4=json.loads(data_link_gb)
                                        if data_resp_link4!=[]:
                                            writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but gb id SM have'])
                                        else:
                                            pass
                                else: 
                                    rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                                    response_link=urllib2.Request(rovi_images_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link3=json.loads(data_link)
                                    if data_resp_link3!=[]:
                                        writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id have'])           
                                    else:
                                        if projectx_show_type[0].encode()=='SE':
                                            mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                            response_link=urllib2.Request(mapping_api)
                                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                            resp_link=urllib2.urlopen(response_link)
                                            data_link=resp_link.read()
                                            data_resp_link=json.loads(data_link) 
                                            if data_resp_link:
                                                for ii in data_resp_link:
                                                    if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                        arr_rovi_px_sm.append(ii.get("source_id"))
                                                    if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                        arr_gb_px_sm.append(ii.get("source_id"))
                                            if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                                rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                                response_link=urllib2.Request(rovi_images_api)
                                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                                resp_link=urllib2.urlopen(response_link)
                                                data_link=resp_link.read()
                                                data_resp_link3=json.loads(data_link)
                                                if data_resp_link3!=[]:
                                                    writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id SM have'])
                                                else:
                                                    pass
                        else:
                   #         import pdb;pdb.set_trace()
                            rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px[0].encode()
                            response_link=urllib2.Request(rovi_images_api)
                            response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_link3=json.loads(data_link)
                            if data_resp_link3!=[]:
                                writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id have'])               
                            else:
                                if projectx_show_type[0].encode()=='SE':
                                    mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"%projectx_series_id
                                    response_link=urllib2.Request(mapping_api)
                                    response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                    resp_link=urllib2.urlopen(response_link)
                                    data_link=resp_link.read()
                                    data_resp_link=json.loads(data_link)
                                    if data_resp_link:
                                        for ii in data_resp_link:
                                            if ii.get("data_source")=="Rovi" and ii.get("type")=='Program':
                                                arr_rovi_px_sm.append(ii.get("source_id"))
                                            if ii.get("data_source")=="GuideBox" and ii.get("type")=='Program':
                                                arr_gb_px_sm.append(ii.get("source_id"))
                                    if (arr_rovi_px_sm!=[] and len(arr_rovi_px_sm)==1) or (arr_gb_px_sm!=[] and len(arr_gb_px_sm)==1):
                                        rovi_images_api="http://34.231.212.186:81/projectx/%s/roviimage"%arr_rovi_px_sm[0].encode()
                                        response_link=urllib2.Request(rovi_images_api)
                                        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                        resp_link=urllib2.urlopen(response_link)
                                        data_link=resp_link.read()
                                        data_resp_link3=json.loads(data_link)
                                        if data_resp_link3!=[]:
                                            writer1.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'this projectx id have no images but rovi id SM have'])
                                        else:
                                            pass         

                if preprod_response_flag=='False' and projectx_response_flag=='False' and preprod_videos_link_flag=='True' and projectx_videos_link_flag=='True':  #  """********************"""
                    if (arr_rovi_px!=[] and arr_gb_px!=[]) or (arr_rovi_px!=[] and arr_gb_px==[]) or (arr_rovi_px==[] and arr_gb_px!=[]):

                        writer.writerow([aa,projectx_show_type,projectx_title,projectx_episode_title,arr_rovi_px,arr_gb_px,prerpod_show_type,preprod_title,preprod_episode_title,preprod_images,projectx_images_flag,preprod_images_flag,image_match,image_url_missing,missing_url,format_mismatch,format_mismatch_,aspectRatio_mismatch,aspectRatio_mismatch_,orientation_mismatch,orientation_mismatch_,imageType_mismatch,imageType_mismatch_,'Projectx id have no response'])

            else:
                pass
            print datetime.datetime.now()


    except httplib.BadStatusLine:
        print ("exception caught httplib.BadStatusLine................................................................................",name,"Projectx ids:",aa)
        print ("\n") 
        print ("Retrying.............")
        print ("\n") 
	get_result_images(aa,name,writer1,writer,total)
    except urllib2.HTTPError:
        print ("exception caught HTTPError................................................................................",name,"Projectx ids:",aa)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        get_result_images(aa,name,writer1,writer,total)    
    except socket.error:
        print ("exception caught SocketError..........................................................................................",name,"Projectx ids:",aa)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        get_result_images(aa,name,writer1,writer,total)   
    except URLError:
        print ("exception caught URLError.............................................................................................",name,"Projectx ids:",aa)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        get_result_images(aa,name,writer1,writer,total)  
    except RuntimeError:
        print ("exception caught ..................................................................................",name,"Projectx ids:",aa)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        get_result_images(aa,name,writer1,writer,total)     
       

def images_checking(start,name,end,id):

    v=''
    result_sheet1='/images_checked_in_projectx_failure_cases%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet1)):
        os.remove(os.getcwd()+result_sheet1)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    v=open(os.getcwd()+result_sheet1,"wa")
    total=0
    result_sheet='/images_checked_in_projectx%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with v as mycsvfile1,w as mycsvfile:
        fieldnames = ["Projectx_id","projectx_show_type","Px_title","Px_episode_title","arr_rovi_px","arr_gb_px","preprod_show_type","preprod_title","preprod_episode_title","preprod_images","projectx_images_flag","preprod_images_flag","image_match","image_url_wrong","wrong_url","format_mismatch","Wrong format","aspectRatio_mismatch","Wrong aspectRatio","orientation_mismatch","Wrong orientation","imageType_mismatch","Wrong imageType","Comment"]

        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer1 = csv.writer(mycsvfile1,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        writer1.writerow(fieldnames)
        for aa in range(start,end):
            total=total+1
            get_result_images(aa,name,writer1,writer,total)

 
t1=threading.Thread(target=images_checking,args=(81,"thread - 1",500001,1))
t1.start()
t2=threading.Thread(target=images_checking,args=(500001,"thread - 2",1000001,2))
t2.start()

t3=threading.Thread(target=images_checking,args=(1000001,"thread - 3",1500001,3))
t3.start()
t4=threading.Thread(target=images_checking,args=(1500001,"thread - 4",2000001,4))
t4.start()
t5=threading.Thread(target=images_checking,args=(2500001,"thread - 5",3000001,5))
t5.start()
t6=threading.Thread(target=images_checking,args=(3000001,"thread - 6",3500001,6))
t6.start() 
t7=threading.Thread(target=images_checking,args=(3500001,"thread - 7",4000001,7))
t7.start() 
t8=threading.Thread(target=images_checking,args=(4000001,"thread - 8",4500001,8))
t8.start() 
t9=threading.Thread(target=images_checking,args=(4500001,"thread - 9",5000001,9))
t9.start()
t10=threading.Thread(target=images_checking,args=(5000001,"thread - 10",5500001,10))
t10.start() 

t11=threading.Thread(target=images_checking,args=(5500001,"thread - 11",6000001,11))
t11.start()
t12=threading.Thread(target=images_checking,args=(6000001,"thread - 12",6500001,12))
t12.start() 
t13=threading.Thread(target=images_checking,args=(6500001,"thread - 13",7000001,13))
t13.start() 
t14=threading.Thread(target=images_checking,args=(7000001,"thread - 14",7500001,14))
t14.start() 
t15=threading.Thread(target=images_checking,args=(7500001,"thread - 15",8000001,15))
t15.start() 
t16=threading.Thread(target=images_checking,args=(8000001,"thread - 16",8500001,16))
t16.start() 
t17=threading.Thread(target=images_checking,args=(8500001,"thread - 17",9000001,17))
t17.start()
t18=threading.Thread(target=images_checking,args=(9000001,"thread - 18",9500001,18))
t18.start()
t19=threading.Thread(target=images_checking,args=(9500001,"thread - 19",10000001,19))
t19.start()
t20=threading.Thread(target=images_checking,args=(100000001,"thread - 20",12500001,20))
t20.start()
t2.join()
t1.join()
t3.join()
t4.join()
t5.join()
t6.join()
t7.join()
t8.join()
t9.join()
t10.join()
t11.join()
t12.join()
t13.join()
t14.join()
t15.join()
t16.join()
t17.join()
t18.join()
t19.join()
t20.join()
#t21.join()

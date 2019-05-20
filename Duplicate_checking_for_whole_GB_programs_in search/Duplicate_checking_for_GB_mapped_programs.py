""" Here we are checking duplicate programs in search api """
"""Saayan"""

import threading
import pymongo
from pprint import pprint
import sys
import os
import csv
import re
import pymysql
import collections
from pprint import pprint
import MySQLdb
import re
import collections
from collections import Counter
from urllib2 import URLError
import socket
import datetime
import _mysql_exceptions
import unidecode
import urllib2
import json
import urllib
import httplib
from urllib2 import HTTPError
import unidecode

def duplicate_checking(gb_id,gb_sm_id,gb_show_type,name,writer,gb_list):
    #import pdb;pdb.set_trace()
    print("\n")
    print (gb_id,gb_sm_id,gb_show_type,name,writer,len(gb_list))
    Token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
    domain_name="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com"
    token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
    gb_tv_everywhere_web_flag=''
    gb_subscription_web_flag=''
    gb_free_web_flag=''
    gb_purchase_web_flag=''
    gb_purchase_web_link=''
    gb_purchase_web_source=''
    gb_subscription_web_source=''
    gb_subscription_web_id=[]
    gb_purchase_web_id=[]
    gb_tv_everywhere_web_source=''
    gb_tv_everywhere_web_link=''
    gb_tv_everywhere_web_id=[]
    gb_free_web_source=''
    gb_free_web_link=''
    gb_free_web_id=[]
    web_link_id=[]
    GB_movie_title=''
    GB_movie_release_year=''
    px_id=[]
    comment=''
    search_px_id__=[]
    comment_link=''
    result=''
    GB_show_id=0
    GB_episode_release_year=''
    GB_episode_title=''
    series_id_px=[]
    gb_series_title=''         
    gb_series_release_year=0
    credit_array=[]
    duplicate=''
    credit_match=''

    try:
        if gb_show_type=='MO':
            GB_details_api_mo="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=MO"%gb_id
            GB_link=urllib2.Request(GB_details_api_mo)
            GB_link.add_header('Authorization',token)
            GB_resp=urllib2.urlopen(GB_link)
            data_GB=GB_resp.read()
            data_GB_resp_mo=json.loads(data_GB)
            GB_movie_title= unidecode.unidecode(data_GB_resp_mo.get("title"))
            GB_movie_release_year= data_GB_resp_mo.get("release_year")

            if data_GB_resp_mo.get("purchase_web_sources"):
                
                for link in range(0,len(data_GB_resp_mo.get("purchase_web_sources"))):#data_GB_resp_mo.get("purchase_web_sources")
                    gb_purchase_web_source=data_GB_resp_mo.get("purchase_web_sources")[link].get("source").encode('utf-8')
                    gb_purchase_web_link=data_GB_resp_mo.get("purchase_web_sources")[link].get("link").encode('utf-8')
                    if 'amazon_prime' in gb_purchase_web_source or 'amazon_buy' in gb_purchase_web_source:
                        gb_purchase_web_source='amazon'
                    if 'netflix' in gb_purchase_web_source:
                        gb_purchase_web_source='netflixusa'
                    if gb_purchase_web_source=='hbo':
                        gb_purchase_web_source='hbogo'
                    if gb_purchase_web_source=='hbo_now':
                        gb_purchase_web_source='hbogo'
                    if gb_purchase_web_source=='google_play':
                        gb_purchase_web_source='googleplay'
                    if gb_purchase_web_source=='hulu_plus':
                        gb_purchase_web_source='hulu'
                    if gb_purchase_web_source =='verizon_on_demand':
                        gb_purchase_web_source='verizon'
                    if gb_purchase_web_source=='showtime_subscription':
                        gb_purchase_web_source='showtime'
                                   
                    if 'vuduapp' in gb_purchase_web_link:
                        gb_purchase_web_id.append(re.findall("\w+.*?", gb_purchase_web_link)[-1:][0])
                    try:
                        if '//itunes.apple.com/us/tv-season' in gb_purchase_web_link:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[-2:-1][0]})
                    except IndexError:
                        try:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[1:-2][0]})
                        except IndexError:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0]})
                    try:
                        if '//itunes.apple.com/us/movie' in gb_purchase_web_link:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0:-2][2:][1]})
                    except IndexError:
                        try:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[1:-2][1]})
                        except IndexError:
                            try:
                                gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+",gb_purchase_web_link)[0:-2][1:2][0]})
                            except IndexError:
                                gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0]})
                    if '//www.amazon.com/gp' in gb_purchase_web_link:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\w+\d+\w+", gb_purchase_web_link)[0]})
                    if '//click.linksynergy.com/' in gb_purchase_web_link:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+.*?", gb_purchase_web_link)[-1:][0]})
                    if gb_purchase_web_id:
                        gb_purchase_web_flag='True'
                        break

            elif data_GB_resp_mo.get("subscription_web_sources"):
                
                for link in range(0,len(data_GB_resp_mo.get("subscription_web_sources"))):
                    gb_subscription_web_source=data_GB_resp_mo.get("subscription_web_sources")[link].get('source').encode('utf-8')
                    gb_subscription_web_link=data_GB_resp_mo.get("subscription_web_sources")[link].get('link').encode('utf-8')

                    if 'amazon_prime' in gb_subscription_web_source or 'amazon_buy' in gb_subscription_web_source:
                        gb_subscription_web_source='amazon'
                    if 'netflix' in gb_subscription_web_source:
                        gb_subscription_web_source='netflixusa'
                    if gb_subscription_web_source=='hbo':
                        gb_subscription_web_source='hbogo'
                    if gb_subscription_web_source=='hbo_now':
                        gb_subscription_web_source='hbogo'
                    if gb_subscription_web_source=='google_play':
                        gb_subscription_web_source='googleplay'
                    if gb_subscription_web_source=='hulu_plus':
                        gb_subscription_web_source='hulu'
                    if gb_subscription_web_source =='verizon_on_demand':
                        gb_subscription_web_source='verizon'
                    if gb_subscription_web_source=='showtime_subscription':
                        gb_subscription_web_source='showtime'               
    
                    if 'vuduapp' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})
                                    
                    if 'aiv://aiv/play' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})
                    if '//itunes.apple.com/us/movie' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\d+", gb_subscription_web_link)[0]})
                    if '//www.amazon.com/gp' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\d+\w+", gb_subscription_web_link)[0]})
                    if "www.cbs.com/shows" in gb_subscription_web_link:
                        try:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\d+\w+", gb_subscription_web_link)[0]})
                        except IndexError:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+", gb_subscription_web_link)[7]})
                    if '//click.linksynergy.com/' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\d+", gb_subscription_web_link)[-1:][0]})
                    if 'play.google' in gb_subscription_web_link:
                        try:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+-\w+.*?",gb_subscription_web_link)[0]})
                        except IndexError:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})                      
                    if '//play.hbonow.com/feature/' in gb_subscription_web_link:
                        try:
                            a10=re.findall("\w+.*?", gb_subscription_web_link)
                            gb_subscription_web_id.append({gb_subscription_web_source:':'.join(map(str, [a10[i] for i in range(5,9)]))})
                        except IndexError:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_subscription_web_link)[0]})                         
                    if 'netflix.com' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0]})
                    if 'http://www.showtime.com/#' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0]})
                    if 'http://www.hulu.com' in gb_subscription_web_link:
                        try:
                            a14=re.findall("\w+.*?",gb_subscription_web_link)[-5:]
                            gb_subscription_web_id.append({gb_subscription_web_source:'-'.join(map(str,[a14[i] for i in range(0,len(a14))]))})
                        except IndexError:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0] })
                    if gb_subscription_web_id:
                        gb_subscription_web_flag='True'
                        break

            elif data_GB_resp_mo.get("tv_everywhere_web_sources"):
              
                for link in range(0,len(data_GB_resp_mo.get("tv_everywhere_web_sources"))):
                    gb_tv_everywhere_web_source=data_GB_resp_mo.get("tv_everywhere_web_sources")[link].get('source').encode('utf-8')
                    gb_tv_everywhere_web_link=data_GB_resp_mo.get("tv_everywhere_web_sources")[link].get('link').encode('utf-8')
                    if 'amazon_prime' in gb_tv_everywhere_web_source or 'amazon_buy' in gb_tv_everywhere_web_source:
                        gb_tv_everywhere_web_source='amazon'
                    if 'netflix' in gb_tv_everywhere_web_source:
                        gb_tv_everywhere_web_source='netflixusa'
                    if gb_tv_everywhere_web_source=='hbo':
                        gb_tv_everywhere_web_source='hbogo'
                    if gb_tv_everywhere_web_source=='hbo_now':
                        gb_tv_everywhere_web_source='hbogo'
                    if gb_tv_everywhere_web_source=='starz_tveverywhere':
                        gb_tv_everywhere_web_source='starz'
                    if "starz://play" in gb_tv_everywhere_web_link or "//www.starz.com/" in gb_tv_everywhere_web_link:
                        gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+.*?", gb_tv_everywhere_web_link)[-1:][0]})
                    if "www.cbs.com/shows" in gb_tv_everywhere_web_link:
                        try: 
                            gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+\d+\w+", gb_tv_everywhere_web_link)[0]})
                        except IndexError:
                            gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+", gb_tv_everywhere_web_link)[7]})                       
                    if gb_tv_everywhere_web_id:
                        gb_tv_everywhere_web_flag='True'
                        break
    
            elif data_GB_resp_mo.get("free_web_sources"):
                
                for link in range(0,len(data_GB_resp_mo.get("free_web_sources"))):
                    gb_free_web_source=data_GB_resp_mo.get("free_web_sources")[link].get('source').encode('utf-8')
                    gb_free_web_link=data_GB_resp_mo.get("free_web_sources")[link].get('link').encode('utf-8')
                    if 'amazon_prime' in gb_free_web_source or 'amazon_buy' in gb_free_web_source:
                        gb_free_web_source='amazon'
                    if 'https://www.vudu.com' in gb_free_web_source:
                        gb_free_web_source='vudu'
                    if '//www.amazon.com/gp' in gb_free_web_link:
                        gb_free_web_id.append({gb_free_web_source:re.findall("\w+\d+\w+", gb_free_web_link)[0]})    
                    if 'https://www.vudu.com' in gb_free_web_link:
                        gb_free_web_id.append({gb_free_web_source:re.findall("\w+.*?",gb_free_web_link)[-3:][0]})
                    if "//play.hbogo.com/feature" in gb_free_web_link:
                        try:
                            gb_free_web_id.append({gb_free_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_free_web_link)[0]})
                        except IndexError:
                            try:
                                a3=re.findall("\w+.*?", gb_free_web_link)
                                gb_free_web_id.append({gb_free_web_source:':'.join(map(str, [a3[i] for i in range(5,9)]))})
                            except IndexError:
                                gb_free_web_id.append({gb_free_web_source:re.findall("\w+.*?",gb_free_web_link)[-1:][0]})
                    if "http://www.nbc.com" in gb_free_web_link:
                        gb_free_web_id.append({gb_free_web_source:re.findall("\d+",gb_free_web_link)[0]})                
                    if gb_free_web_id:
                        gb_free_web_flag='True'
                        break
            if gb_free_web_flag=='True' or gb_tv_everywhere_web_flag=='True' or gb_purchase_web_flag=='True' or gb_subscription_web_flag=='True':
                web_link_id=gb_purchase_web_id+gb_subscription_web_id+gb_tv_everywhere_web_id+gb_free_web_id
            else:
                comment_link='No link in scope'
                web_link_id=[]     
            if comment_link!="No link in scope":
                reverse_api="http://34.231.212.186:81/projectx/%s/%s/ottprojectx"%(web_link_id[0].get(web_link_id[0].keys()[0]),web_link_id[0].keys()[0])
                reverse_api_data=urllib2.Request(reverse_api)
                reverse_api_data.add_header('Authorization',token)
                reverse_api_data_resp=urllib2.urlopen(reverse_api_data)
                data_reverse_api=reverse_api_data_resp.read()
                data_reverse_api_resp=json.loads(data_reverse_api)
                for kk in data_reverse_api_resp:
                    if kk.get("data_source")=='GuideBox' and kk.get("type")=='Program' and kk.get("sub_type")=='MO':
                        px_id.append(kk.get("projectx_id"))
                    if kk.get("data_source")=='Rovi' and kk.get("type")=='Program' and kk.get("sub_type")=='ALL':
                        px_id.append(kk.get("projectx_id"))
                if len(px_id)>1:
                    for bb in px_id:
                        while px_id.count(bb)>1:
                            px_id.remove(bb)
                if len(px_id)>1:
                    arr_gb=[]
                    arr_rovi=[]
                    search_px_id_=[]
                    search_px_id=[]
                    search_px_id__=[]
                    search_px_id1_=[]
                    search_px_id1=[]
                    search_px_id_filtered=[]
                    next_page_url=""
                    duplicate="" 
                    data_resp_search=dict()
                    #import pdb;pdb.set_trace() 
                    search_api="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/v3/voice_search?q=%s&safe_search=false&credit_summary=true&credit_types=Actor&aliases=true&ott=true"%urllib2.quote(GB_movie_title)
                    response_search=urllib2.Request(search_api)
                    response_search.add_header('User-Agent','Branch Fyra v1.0')
                    response_search.add_header('Authorization',token) 
                    resp_search=urllib2.urlopen(response_search)
                    data_search=resp_search.read()
                    data_resp_search=json.loads(data_search)
                    if data_resp_search.get("top_results"):
                        for ii in data_resp_search.get("top_results"):
                            if ii.get("action_type")=="ott_search" and ii.get("action_type")!="web_results" and ii.get("results"):
                                for jj in ii.get("results"):
                                    if jj.get("object").get("show_type")=='MO' or jj.get("object").get("show_type")=='OT':
                                        search_px_id.append(jj.get("object").get("id"))
                                        search_px_id1=search_px_id1+search_px_id
     
                        if search_px_id:
                            for mm in search_px_id:
                                if mm in px_id:
                                    search_px_id_.append(mm)
                                else:
                                    search_px_id_filtered.append(mm)


 
                        if len(search_px_id_)==1 or search_px_id_==[]:
                            try:
                                search_px_id1_.append(search_px_id_[0])
                                search_px_id_=[]
                                search_px_id=[]
                                duplicate='False'
                            except IndexError:
                                search_px_id_=[]
                                search_px_id=[]
                                duplicate='False'
                        else:
                            if search_px_id_!=search_px_id__:
                                search_px_id__=search_px_id__+search_px_id_
                                duplicate='True'
                                search_px_id=[]
                            else:
                                search_px_id__=search_px_id__
                                duplicate='True'
                                search_px_id=[] 

                        if duplicate=='False': 
                            while data_resp_search.get("results"):
                                for nn in data_resp_search.get("results"):
                                    if nn.get("action_type")=="ott_search" and (nn.get("results")==[] or nn.get("results")):
                                        next_page_url=nn.get("next_page_url")
                                        if next_page_url is not None: 
                                            search_api1=domain_name+next_page_url.replace(' ',"%20")
                                            if search_api1!=domain_name :
                                                search_api=search_api1
                                                response_search=urllib2.Request(search_api)
                                                response_search.add_header('User-Agent','Branch Fyra v1.0')
                                                response_search.add_header('Authorization',token)
                                                resp_search=urllib2.urlopen(response_search)
                                                data_search=resp_search.read()
                                                data_resp_search=json.loads(data_search)
                                        else:
                                            data_resp_search={"resilts":[]}         
                                    else:
                                        data_resp_search={"resilts":[]}

                                    if data_resp_search.get("results"):
                                        for nn in data_resp_search.get('results'):
                                            if nn.get("results"):
                                                for jj in nn.get("results"):
                                                    if jj.get("object").get("show_type")=='MO' or jj.get("object").get("show_type")=='OT':
                                                        search_px_id.append(jj.get("object").get("id"))
                                                        search_px_id1=search_px_id1+search_px_id  

                                            if search_px_id:
                                                for mm in search_px_id:
                                                    if mm in px_id:
                                                        search_px_id_.append(mm)
                                                    else:
                                                        search_px_id_filtered.append(mm)
                                    
                                            for ss in search_px_id_:
                                                while search_px_id_.count(ss)>1:
                                                    search_px_id_.remove(ss)

                                            for ss in search_px_id1_:
                                                while search_px_id1_.count(ss)>1:
                                                    search_px_id1_.remove(ss)
                                                while search_px_id1_.count(ss)>1:
                                                    search_px_id1_.remove(ss) 

                                            if len(search_px_id_)==1 or search_px_id_==[]:
                                                try:
                                                    search_px_id1_.append(search_px_id_[0]) 
                                                    search_px_id_=[] 
                                                    search_px_id=[] 
                                                    duplicate='False' 
                                                except IndexError:
                                                    search_px_id_=[]
                                                    search_px_id=[]
                                                    duplicate='False'      
                                            else:
                                                if search_px_id_!=search_px_id__:
                                                    search_px_id__=search_px_id__+search_px_id_
                                                    duplicate='True'
                                                    search_px_id=[]
                                                else:
                                                    search_px_id__=search_px_id__
                                                    duplicate='True'
                                                    search_px_id=[] 

                    if search_px_id1:
                        if len(search_px_id__)>1 and duplicate=='True':
                            px_link="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true&aliases=true" %'{}'.format(",".join([str(i) for i in search_px_id__]))
                            response_link=urllib2.Request(px_link)
                            response_link.add_header('Authorization',token)
                            resp_link=urllib2.urlopen(response_link)
                            data_link=resp_link.read()
                            data_resp_credits=json.loads(data_link)
                            for uu in data_resp_credits:
                                if uu.get("credits"):
                                    for tt in uu.get("credits"):
                                        credit_array.append(unidecode.unidecode(tt.get("full_credit_name")))
                            if credit_array:
                                for cc in credit_array:
                                    if credit_array.count(cc)>1:
                                        credit_match='True'
                                        break
                                    else:
                                        credit_match='False'
                            else:
                                credit_match='True'             
                            if credit_match=="True": 
                                comment='Duplicate projectx ids found in search api'
                                writer.writerow([gb_sm_id,gb_id,gb_show_type,'','',GB_movie_title,'','',GB_movie_release_year,gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,comment,search_px_id__,credit_match])
                            else:
                                comment='Duplicate projectx ids found in search api'
                                writer.writerow([gb_sm_id,gb_id,gb_show_type,'','',GB_movie_title,'','',GB_movie_release_year,gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,comment,search_px_id__,credit_match])
                        else:
                            comment='Duplicate ids not found in search api' 
                            writer.writerow([gb_sm_id,gb_id,gb_show_type,'','',GB_movie_title,'','',GB_movie_release_year,gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,comment,search_px_id__,''])            
                    else:
                        if 'Open' in GB_movie_title or 'Season' in GB_movie_title or 'season' in GB_movie_title or 'Part' in GB_movie_title:
                            pass
                        else:

                            duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=MO"%gb_id
                            response_duplicate=urllib2.Request(duplicate_api)
                            response_duplicate.add_header('Authorization',token)
                            resp_duplicate=urllib2.urlopen(response_duplicate)
                            data_duplicate=resp_link.read()
                            data_resp_duplicate=json.loads(data_duplicate)
                            if data_resp_duplicate:
                                duplicate='True'
                            else:
                                duplicate='False'                                
                            comment='Search api has no response'
                            writer.writerow([gb_sm_id,gb_id,gb_show_type,'','',GB_movie_title,'','',GB_movie_release_year,gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,comment,search_px_id__,'',duplicate])
                else:
                    comment=('No multiple ids for this link',web_link_id[0])
                    result="No multiple ids for this link"
                    writer.writerow([gb_sm_id,gb_id,gb_show_type,'','',GB_movie_title,'','',GB_movie_release_year,gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,result,search_px_id__,''])
            else:
                result=comment_link
                writer.writerow([gb_sm_id,gb_id,gb_show_type,'','',GB_movie_title,'','',GB_movie_release_year,gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment_link,result,search_px_id__,''])                                    
        if gb_show_type=='SE':
            #import pdb;pdb.set_trace()
            GB_details_api_se="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=SE"%gb_id
            GB_link=urllib2.Request(GB_details_api_se)
            GB_link.add_header('Authorization',token)
            GB_resp=urllib2.urlopen(GB_link)
            data_GB=GB_resp.read()
            data_GB_resp_se=json.loads(data_GB)
            GB_episode_title= unidecode.unidecode(data_GB_resp_se.get("title"))
            GB_episode_release_year= data_GB_resp_se.get("release_year")
            GB_show_id= data_GB_resp_se.get("show_id")
                                  
            if data_GB_resp_se.get("purchase_web_sources"):
                
                for link in range(0,len(data_GB_resp_se.get("purchase_web_sources"))):
                    gb_purchase_web_source=data_GB_resp_se.get("purchase_web_sources")[link].get("source").encode('utf-8')
                    gb_purchase_web_link=data_GB_resp_se.get("purchase_web_sources")[link].get("link").encode('utf-8')
                    if 'amazon_prime' in gb_purchase_web_source or 'amazon_buy' in gb_purchase_web_source:
                        gb_purchase_web_source='amazon'
                    if 'netflix' in gb_purchase_web_source:
                        gb_purchase_web_source='netflixusa'
                    if gb_purchase_web_source=='hbo':
                        gb_purchase_web_source='hbogo'
                    if gb_purchase_web_source=='hbo_now':
                        gb_purchase_web_source='hbogo'
                    if gb_purchase_web_source=='google_play':
                        gb_purchase_web_source='googleplay'
                    if gb_purchase_web_source=='hulu_plus':
                        gb_purchase_web_source='hulu'
                    if gb_purchase_web_source =='verizon_on_demand':
                        gb_purchase_web_source='verizon'
                    if gb_purchase_web_source=='showtime_subscription':
                        gb_purchase_web_source='showtime'
                                   
                    if 'vuduapp' in gb_purchase_web_link:
                        gb_purchase_web_id.append(re.findall("\w+.*?", gb_purchase_web_link)[-1:][0])
                    try:
                        if '//itunes.apple.com/us/tv-season' in gb_purchase_web_link:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[-2:-1][0]})
                    except IndexError:
                        try:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[1:-2][0]})
                        except IndexError:
                            gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+", gb_purchase_web_link)[0]})
                    
                    if '//www.amazon.com/gp' in gb_purchase_web_link:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\w+\d+\w+", gb_purchase_web_link)[0]})
                    if '//click.linksynergy.com/' in gb_purchase_web_link:
                        gb_purchase_web_id.append({gb_purchase_web_source:re.findall("\d+.*?", gb_purchase_web_link)[-1:][0]})
                    if gb_purchase_web_id:
                        gb_purchase_web_flag='True'
                        break

            elif data_GB_resp_se.get("subscription_web_sources"):
                
                for link in range(0,len(data_GB_resp_se.get("subscription_web_sources"))):
                    gb_subscription_web_source=data_GB_resp_se.get("subscription_web_sources")[link].get('source').encode('utf-8')
                    gb_subscription_web_link=data_GB_resp_se.get("subscription_web_sources")[link].get('link').encode('utf-8')

                    if 'amazon_prime' in gb_subscription_web_source or 'amazon_buy' in gb_subscription_web_source:
                        gb_subscription_web_source='amazon'
                    if 'netflix' in gb_subscription_web_source:
                        gb_subscription_web_source='netflixusa'
                    if gb_subscription_web_source=='hbo':
                        gb_subscription_web_source='hbogo'
                    if gb_subscription_web_source=='hbo_now':
                        gb_subscription_web_source='hbogo'
                    if gb_subscription_web_source=='google_play':
                        gb_subscription_web_source='googleplay'
                    if gb_subscription_web_source=='hulu_plus':
                        gb_subscription_web_source='hulu'
                    if gb_subscription_web_source =='verizon_on_demand':
                        gb_subscription_web_source='verizon'
                    if gb_subscription_web_source=='showtime_subscription':
                        gb_subscription_web_source='showtime'               
    
                    if 'vuduapp' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})
                                    
                    if 'aiv://aiv/play' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})
                    if '//itunes.apple.com/us/movie' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\d+", gb_subscription_web_link)[0]})
                    if '//www.amazon.com/gp' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\d+\w+", gb_subscription_web_link)[0]})
                    if "www.cbs.com/shows" in gb_subscription_web_link:
                        try:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\d+\w+", gb_subscription_web_link)[0]})
                        except IndexError:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+", gb_subscription_web_link)[7]})
                    if '//click.linksynergy.com/' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\d+", gb_subscription_web_link)[-1:][0]})
                    if 'play.google' in gb_subscription_web_link:
                        try:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+-\w+.*?",gb_subscription_web_link)[0]})
                        except IndexError:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?", gb_subscription_web_link)[-1:][0]})                      
                    if '//play.hbonow.com/' in gb_subscription_web_link:
                        try:
                            a10=re.findall("\w+.*?", gb_subscription_web_link)
                            gb_subscription_web_id.append({gb_subscription_web_source:':'.join(map(str, [a10[i] for i in range(5,9)]))})
                        except IndexError:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_subscription_web_link)[0]})                         
                    if 'netflix.com' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0]})
                    if 'http://www.showtime.com/#' in gb_subscription_web_link:
                        gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0]})
                    if 'http://www.hulu.com' in gb_subscription_web_link:
                        try:
                            a14=re.findall("\w+.*?",gb_subscription_web_link)[-5:]
                            gb_subscription_web_id.append({gb_subscription_web_source:'-'.join(map(str,[a14[i] for i in range(0,len(a14))]))})
                        except IndexError:
                            gb_subscription_web_id.append({gb_subscription_web_source:re.findall("\w+.*?",gb_subscription_web_link)[-1:][0] })
                    if gb_subscription_web_id:
                        gb_subscription_web_flag='True'
                        break

            elif data_GB_resp_se.get("tv_everywhere_web_sources"):
              
                for link in range(0,len(data_GB_resp_se.get("tv_everywhere_web_sources"))):
                    gb_tv_everywhere_web_source=data_GB_resp_se.get("tv_everywhere_web_sources")[link].get('source').encode('utf-8')
                    gb_tv_everywhere_web_link=data_GB_resp_se.get("tv_everywhere_web_sources")[link].get('link').encode('utf-8')
                    if 'amazon_prime' in gb_tv_everywhere_web_source or 'amazon_buy' in gb_tv_everywhere_web_source:
                        gb_tv_everywhere_web_source='amazon'
                    if 'netflix' in gb_tv_everywhere_web_source:
                        gb_tv_everywhere_web_source='netflixusa'
                    if gb_tv_everywhere_web_source=='hbo':
                        gb_tv_everywhere_web_source='hbogo'
                    if gb_tv_everywhere_web_source=='hbo_now':
                        gb_tv_everywhere_web_source='hbogo'
                    if gb_tv_everywhere_web_source=='starz_tveverywhere':
                        gb_tv_everywhere_web_source='starz'
                    if "starz://play" in gb_tv_everywhere_web_link or "//www.starz.com/" in gb_tv_everywhere_web_link:
                        gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+.*?", gb_tv_everywhere_web_link)[-1:][0]})
                    if "www.cbs.com/shows" in gb_tv_everywhere_web_link:
                        try: 
                            gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+\d+\w+", gb_tv_everywhere_web_link)[0]})
                        except IndexError:
                            gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+", gb_tv_everywhere_web_link)[7]})                       
                    if '//play.hbonow.com/' in gb_tv_everywhere_web_link:
                        try:
                            a10=re.findall("\w+.*?", gb_tv_everywhere_web_link)
                            gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:':'.join(map(str, [a10[i] for i in range(5,9)]))})
                        except IndexError:
                            gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_tv_everywhere_web_link)[0]})        
                    if '/play.hbogo.com/episode/' in gb_tv_everywhere_web_link:
                        try:
                            a6=re.findall("\w+.*?.-*?", gb_tv_everywhere_web_link)
                            gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:''.join(map(str, [a6[i] for i in range(5,len(a6))]))})
                        except IndexError:
                            try:
                                gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_tv_everywhere_web_link)[0]})
                            except IndexError:
                                gb_tv_everywhere_web_id.append({gb_tv_everywhere_web_source:re.findall("\w+.*?",gb_tv_everywhere_web_link)[-1:][0]})
                    if gb_tv_everywhere_web_id:
                        gb_tv_everywhere_web_flag='True'
                        break
    
            elif data_GB_resp_se.get("free_web_sources"):
                
                for link in range(0,len(data_GB_resp_se.get("free_web_sources"))):
                    gb_free_web_source=data_GB_resp_se.get("free_web_sources")[link].get('source').encode('utf-8')
                    gb_free_web_link=data_GB_resp_se.get("free_web_sources")[link].get('link').encode('utf-8')
                    if 'amazon_prime' in gb_free_web_source or 'amazon_buy' in gb_free_web_source:
                        gb_free_web_source='amazon'
                    if 'https://www.vudu.com' in gb_free_web_source:
                        gb_free_web_source='vudu'
                    if '//www.amazon.com/gp' in gb_free_web_link:
                        gb_free_web_id.append({gb_free_web_source:re.findall("\w+\d+\w+", gb_free_web_link)[0]})    
                    if 'https://www.vudu.com' in gb_free_web_link:
                        gb_free_web_id.append({gb_free_web_source:re.findall("\w+.*?",gb_free_web_link)[-3:][0]})
                    if "//play.hbogo.com/feature" in gb_free_web_link:
                        try:
                            gb_free_web_id.append({gb_free_web_source:re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", gb_free_web_link)[0]})
                        except IndexError:
                            try:
                                a3=re.findall("\w+.*?", gb_free_web_link)
                                gb_free_web_id.append({gb_free_web_source:':'.join(map(str, [a3[i] for i in range(5,9)]))})
                            except IndexError:
                                gb_free_web_id.append({gb_free_web_source:re.findall("\w+.*?",gb_free_web_link)[-1:][0]})
                    if "http://www.nbc.com" in gb_free_web_link:
                        gb_free_web_id.append({gb_free_web_source:re.findall("\d+",gb_free_web_link)[0]})                
                    if gb_free_web_id:
                        gb_free_web_flag='True'
                        break
            if gb_free_web_flag=='True' or gb_tv_everywhere_web_flag=='True' or gb_purchase_web_flag=='True' or gb_subscription_web_flag=='True':
                web_link_id=gb_purchase_web_id+gb_subscription_web_id+gb_tv_everywhere_web_id+gb_free_web_id
            else:
                comment_link='No link in scope'
                web_link_id=[]     
            if comment_link!="No link in scope":
                reverse_api="http://34.231.212.186:81/projectx/%s/%s/ottprojectx"%(web_link_id[0].get(web_link_id[0].keys()[0]),web_link_id[0].keys()[0])
                reverse_api_data=urllib2.Request(reverse_api)
                reverse_api_data.add_header('Authorization',token)
                reverse_api_data_resp=urllib2.urlopen(reverse_api_data)
                data_reverse_api=reverse_api_data_resp.read()
                data_reverse_api_resp=json.loads(data_reverse_api)
                for kk in data_reverse_api_resp:
                    if kk.get("data_source")=='GuideBox' and kk.get("type")=='Program' and kk.get("sub_type")=='SE':
                        px_id.append(kk.get("projectx_id"))
                    if kk.get("data_source")=='Rovi' and kk.get("type")=='Program' and kk.get("sub_type")=='ALL':
                        px_id.append(kk.get("projectx_id"))
                if len(px_id)>1:
                    for bb in px_id:
                        while px_id.count(bb)>1:
                            px_id.remove(bb)
                GB_details_api_sm="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=SM"%GB_show_id
                GB_link=urllib2.Request(GB_details_api_sm)
                GB_link.add_header('Authorization',token)
                GB_resp=urllib2.urlopen(GB_link)
                data_GB=GB_resp.read()
                data_GB_resp_sm=json.loads(data_GB)
                                                          
                gb_series_title=unidecode.unidecode(data_GB_resp_sm.get("title"))
                if gb_series_title is None or gb_series_title =='':
                    gb_series_title= unidecode.unidecode(data_GB_resp_sm.get("original_title"))           
                gb_series_release_year=data_GB_resp_sm.get("release_year")
                                                                                      
                if len(px_id)>1:
                    
                    search_px_id_=[]
                    search_px_id=[]
                    search_px_id__=[]
                    search_px_id1_=[]
                    search_px_id1=[]
                    search_px_id_filtered=[]
                    next_page_url=""
                    duplicate="" 
                    data_resp_search=dict()

                    px_link="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true&aliases=true" %'{}'.format(",".join([str(i) for i in px_id]))
                    response_link=urllib2.Request(px_link)
                    response_link.add_header('Authorization',token)
                    resp_link=urllib2.urlopen(response_link)
                    data_link=resp_link.read()
                    data_resp_link3=json.loads(data_link)
                    for kk in data_resp_link3:
                        series_id_px.append(kk.get("series_id"))
                    for ll in series_id_px:
                        while series_id_px.count(ll)>1:
                            series_id_px.remove(ll)
    #                             import pdb;pdb.set_trace() 
                    if len(series_id_px) >1:
                        search_px_id=[]
                        search_api="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/v3/voice_search?q=%s&safe_search=false&credit_summary=true&credit_types=Actor&aliases=true&ott=true"%urllib2.quote(gb_series_title)
                        response_search=urllib2.Request(search_api)
                        response_search.add_header('User-Agent','Branch Fyra v1.0')
                        response_search.add_header('Authorization',token) 
                        resp_search=urllib2.urlopen(response_search)
                        data_search=resp_search.read()
                        data_resp_search=json.loads(data_search)
                        if data_resp_search.get("top_results"):
                            for ii in data_resp_search.get("top_results"):
                                if ii.get("action_type")=="ott_search" and ii.get("action_type")!="web_results" and ii.get("results"):
                                    for jj in ii.get("results"):
                                        if jj.get("object").get("show_type")=='SM':
                                            search_px_id.append(jj.get("object").get("id"))
                                            search_px_id1=search_px_id1+search_px_id
                                                                                                                                                                                                         
                            if search_px_id:
                                for mm in search_px_id:
                                    if mm in series_id_px:
                                        search_px_id_.append(mm)
                                    else:
                                        search_px_id_filtered.append(mm)


     
                            if len(search_px_id_)==1 or search_px_id_==[]:
                                try:
                                    search_px_id1_.append(search_px_id_[0])
                                    search_px_id_=[]
                                    search_px_id=[]
                                    duplicate='False'
                                except IndexError:
                                    search_px_id_=[]
                                    search_px_id=[]
                                    duplicate='False'
                            else:
                                if search_px_id_!=search_px_id__:
                                    search_px_id__=search_px_id__+search_px_id_
                                    duplicate='True'
                                    search_px_id=[]
                                else:
                                    search_px_id__=search_px_id__
                                    duplicate='True'
                                    search_px_id=[] 

                            if duplicate=='False': 
                                while data_resp_search.get("results"):
                                    for nn in data_resp_search.get("results"):
                                        if nn.get("action_type")=="ott_search" and (nn.get("results")==[] or nn.get("results")):
                                            next_page_url=nn.get("next_page_url")
                                            if next_page_url is not None: 
                                                search_api1=domain_name+next_page_url.replace(' ',"%20")
                                                if search_api1!=domain_name :
                                                    search_api=search_api1
                                                    response_search=urllib2.Request(search_api)
                                                    response_search.add_header('User-Agent','Branch Fyra v1.0')
                                                    response_search.add_header('Authorization',token)
                                                    resp_search=urllib2.urlopen(response_search)
                                                    data_search=resp_search.read()
                                                    data_resp_search=json.loads(data_search)
                                            else:
                                                data_resp_search={"resilts":[]}         
                                        else:
                                            data_resp_search={"resilts":[]}
     
                                        if data_resp_search.get("results"):
                                            for nn in data_resp_search.get('results'):
                                                if nn.get("results"):
                                                    for jj in nn.get("results"):
                                                        if jj.get("object").get("show_type")=='SM':
                                                            search_px_id.append(jj.get("object").get("id"))
                                                            search_px_id1=search_px_id1+search_px_id  

                                                if search_px_id:
                                                    for mm in search_px_id:
                                                        if mm in series_id_px:
                                                            search_px_id_.append(mm)
                                                        else:
                                                            search_px_id_filtered.append(mm)
                                        
                                                for ss in search_px_id_:
                                                    while search_px_id_.count(ss)>1:
                                                        search_px_id_.remove(ss)

                                                for ss in search_px_id1_:
                                                    while search_px_id1_.count(ss)>1:
                                                        search_px_id1_.remove(ss)
                                                    while search_px_id1_.count(ss)>1:
                                                        search_px_id1_.remove(ss) 

                                                if len(search_px_id_)==1 or search_px_id_==[]:
                                                    try:
                                                        search_px_id1_.append(search_px_id_[0]) 
                                                        search_px_id_=[] 
                                                        search_px_id=[] 
                                                        duplicate='False' 
                                                    except IndexError:
                                                        search_px_id_=[]
                                                        search_px_id=[]
                                                        duplicate='False'      
                                                else:
                                                    if search_px_id_!=search_px_id__:
                                                        search_px_id__=search_px_id__+search_px_id_
                                                        duplicate='True'
                                                        search_px_id=[]
                                                    else:
                                                        search_px_id__=search_px_id__
                                                        duplicate='True'
                                                        search_px_id=[] 
                        if search_px_id1:
                            if len(search_px_id__)>1 and duplicate=='True':
                                px_link="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true&aliases=true" %'{}'.format(",".join([str(i) for i in search_px_id__]))
                                response_link=urllib2.Request(px_link)
                                response_link.add_header('Authorization',token)
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_credits=json.loads(data_link)
                                for uu in data_resp_credits:
                                    if uu.get("credits"):
                                        for tt in uu.get("credits"):
                                            credit_array.append(unidecode.unidecode(tt.get("full_credit_name")))
                                if credit_array:
                                    for cc in credit_array:
                                        if credit_array.count(cc)>1:
                                            credit_match='True'
                                            break
                                        else:
                                            credit_match='False'
                                else:
                                    credit_match='True'             
                                if credit_match=="True": 
                                    comment='Duplicate projectx ids found in search api'
                                    writer.writerow([gb_sm_id,gb_id,gb_show_type,gb_series_title,GB_episode_title,'',gb_series_release_year,GB_episode_release_year,'',gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,comment,search_px_id__,credit_match])
                                else:
                                    comment='Duplicate projectx ids found in search api'
                                    writer.writerow([gb_sm_id,gb_id,gb_show_type,gb_series_title,GB_episode_title,'',gb_series_release_year,GB_episode_release_year,'',gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,comment,search_px_id__,credit_match])                                       
                            else:
                                comment='Duplicate not found for series in search api' 
                                writer.writerow([gb_sm_id,gb_id,gb_show_type,gb_series_title,GB_episode_title,'',gb_series_release_year,GB_episode_release_year,'',gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,comment,search_px_id__])            
                        else:
                            duplicate_api="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=GuideBox&showType=SM"%gb_sm_id
                            response_duplicate=urllib2.Request(duplicate_api)
                            response_duplicate.add_header('Authorization',token)
                            resp_duplicate=urllib2.urlopen(response_duplicate)
                            data_duplicate=resp_link.read()
                            data_resp_duplicate=json.loads(data_duplicate)
                            if data_resp_duplicate:
                                duplicate='True'
                            else:
                                duplicate='False'
                            comment='Search api has no response for series'
                            writer.writerow([gb_sm_id,gb_id,gb_show_type,gb_series_title,GB_episode_title,'',gb_series_release_year,GB_episode_release_year,'',gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,comment,search_px_id__,duplicate])                              
                    else:
                        comment=('No multiple ids for series this link',web_link_id[0])
                        result="No multiple ids for this link"
                        writer.writerow([gb_sm_id,gb_id,gb_show_type,gb_series_title,GB_episode_title,'',gb_series_release_year,GB_episode_release_year,'',gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,result,search_px_id__])
                else:
                    comment=('No multiple ids for episode this link',web_link_id[0])
                    result="No multiple ids for this link"
                    writer.writerow([gb_sm_id,gb_id,gb_show_type,gb_series_title,GB_episode_title,'',gb_series_release_year,GB_episode_release_year,'',gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment,result,search_px_id__])            
            else:
                result=comment_link
                writer.writerow([gb_sm_id,gb_id,gb_show_type,gb_series_title,GB_episode_title,'',gb_series_release_year,GB_episode_release_year,'',gb_purchase_web_id,gb_subscription_web_id,gb_tv_everywhere_web_id,gb_free_web_id,px_id,comment_link,result,search_px_id__])

    except httplib.BadStatusLine:
        print ("exception caught httplib.BadStatusLine................................................................................",name)
        print ("\n") 
        print ("Retrying.............")
        print ("\n") 
        duplicate_checking(gb_id,gb_sm_id,gb_show_type,name,writer,gb_list)
    except urllib2.HTTPError:
        print ("exception caught HTTPError................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        duplicate_checking(gb_id,gb_sm_id,gb_show_type,name,writer,gb_list)
    except socket.error:
        print ("exception caught SocketError..........................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        duplicate_checking(gb_id,gb_sm_id,gb_show_type,name,writer,gb_list)   
    except URLError:
        print ("exception caught URLError.............................................................................................",name)
        print ("\n")
        print ("Retrying.............")
        print ("\n") 
        duplicate_checking(gb_id,gb_sm_id,gb_show_type,name,writer,gb_list)
    except RuntimeError:
        print ("exception caught ..................................................................................",name,)
        print ("\n")
        print ("Retrying.............")
        print ("\n")
        duplicate_checking(gb_id,gb_sm_id,gb_show_type,name,writer,gb_list)                         
                    


                      
def getting_ids(start,name,end,id):
    print name
    print("Checking duplicates of series and Movies to Projectx search api................")

    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["ozone"]
    mytable=mydb["guidebox_program_details"]

    result_sheet='/duplicate_checked_in_search_api%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with w as mycsvfile:
        fieldnames = ["gb_sm_id","gb_id","gb_show_type","GB_series_title","GB_episode_title","GB_movie_title","gb_series_release_year","GB_episode_release_year","GB_movie_release_year","gb_purchase_web_id","gb_subscription_web_id","gb_tv_everywhere_web_id","gb_free_web_id","Projectx_ids","comment","Result",'Duplicate ids in search',"Credit_match","Duplicate gb_id"]

        writer = csv.writer(mycsvfile,dialect="excel",lineterminator = '\n')
        writer.writerow(fieldnames)
        gb_mo_list=[]
        gb_sm_list=[]
        for aa in range(start,end,1000):
            GB_query=res1=mytable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"$and":[{"show_type":{"$in":["SE","MO"]}}]}},{"$project":{"gb_id":1,"show_id":1,"_id":0,"show_type":1}}]) 
            print ("\n")
            print({"start": start,"end":end})

            for i in GB_query:
                #import pdb;pdb.set_trace()
                gb_id=i.get("gb_id")
                gb_sm_id=i.get("show_id")
                gb_show_type=i.get("show_type").encode('utf-8')
                if gb_id is not None:
                    if gb_show_type=='SE':
                        if gb_sm_id not in gb_sm_list:
                            gb_sm_list.append(gb_sm_id)
                            duplicate_checking(gb_id,gb_sm_id,gb_show_type,name,writer,gb_sm_list)
                        else:
                            break
                    else:
                        if gb_id not in gb_mo_list:
                            gb_mo_list.append(gb_id)
                            duplicate_checking(gb_id,gb_sm_id,gb_show_type,name,writer,gb_mo_list)
                        else:
                            break
                    print("\n")                             
                    print ({"Total MO":len(gb_mo_list),"Thread_name":name})
                    print("\n")
                    print ({"Total SM":len(gb_sm_list),"Thread_name":name}) 
                            
                
                              
t1=threading.Thread(target=getting_ids,args=(1,"thread - 1",100001,1))
t1.start()
t2=threading.Thread(target=getting_ids,args=(100001,"thread - 2",200001,2))
t2.start()                          
t3=threading.Thread(target=getting_ids,args=(200001,"thread - 3",300001,3))
t3.start()
t4=threading.Thread(target=getting_ids,args=(400001,"thread - 4",500001,4))
t4.start()
t5=threading.Thread(target=getting_ids,args=(500001,"thread - 5",600001,5))
t5.start()
t6=threading.Thread(target=getting_ids,args=(600001,"thread - 6",700001,6))
t6.start()

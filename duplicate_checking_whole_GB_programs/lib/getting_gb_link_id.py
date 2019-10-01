"""Writer: Saayan"""

import sys
import os
import datetime
import sys
import urllib2
import os
from urllib2 import HTTPError
import httplib
import socket
import pinyin
import unidecode
import re
sys.path.insert(0,os.getcwd()+'/lib')
import initialization_file
import getting_gb_link_id

def getting_purchase_link_ids(data_GB_resp):
    #import pdb;pdb.set_trace()
    gb_purchase_web_link=''
    gb_purchase_web_source=''
    gb_purchase_web_id=[]
            
    for link in range(0,len(data_GB_resp.get("purchase_web_sources"))):#data_GB_resp.get("purchase_web_sources")
        gb_purchase_web_source=data_GB_resp.get("purchase_web_sources")[link].get("source").encode('utf-8')
        gb_purchase_web_link=data_GB_resp.get("purchase_web_sources")[link].get("link").encode('utf-8')
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
            return gb_purchase_web_id
            break
            

def getting_link_subscription_ids(data_GB_resp): 

    gb_subscription_web_source=''
    gb_subscription_web_id=[]
    #import pdb;pdb.set_trace()
    for link in range(0,len(data_GB_resp.get("subscription_web_sources"))):
        gb_subscription_web_source=data_GB_resp.get("subscription_web_sources")[link].get('source').encode('utf-8')
        gb_subscription_web_link=data_GB_resp.get("subscription_web_sources")[link].get('link').encode('utf-8')

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
                gb_subscription_web_id.append({gb_subscription_web_source:':'.join(map(str, [a10[i] for i in range(5,9)]))  })
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
            return gb_subscription_web_id
            break

def getting_link_tveverywhere_ids(data_GB_resp):

    gb_tv_everywhere_web_source=''
    gb_tv_everywhere_web_link=''
    gb_tv_everywhere_web_id=[]

      
    for link in range(0,len(data_GB_resp.get("tv_everywhere_web_sources"))):
        gb_tv_everywhere_web_source=data_GB_resp.get("tv_everywhere_web_sources")[link].get('source').encode('utf-8')
        gb_tv_everywhere_web_link=data_GB_resp.get("tv_everywhere_web_sources")[link].get('link').encode('utf-8')
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
            return gb_tv_everywhere_web_id
            break

def getting_link_free_web_ids(data_GB_resp):

    gb_free_web_source=''
    gb_free_web_link=''
    gb_free_web_id=[]
        
    for link in range(0,len(data_GB_resp.get("free_web_sources"))):
        gb_free_web_source=data_GB_resp.get("free_web_sources")[link].get('source').encode('utf-8')
        gb_free_web_link=data_GB_resp.get("free_web_sources")[link].get('link').encode('utf-8')
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
            return gb_free_web_id
            break




import pymongo
from pprint import pprint
import sys
import os
import csv
import pymysql
import collections
from pprint import pprint
import MySQLdb
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
import re
import unidecode
import threading
   
def id_search_amazon(start,name,end,id):

    connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
    mydb=connection["ozone"]
    mytable=mydb["guidebox_program_details"]

    result_sheet='/reverse_api_checked_netflix_guidebox_same_source_id_API%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with w as mycsvfile:
	fieldnames = ["gb_id","show_type","title","description","original_title","series_title","alternate_titles","season_number","episode_number","release_year","air_date","duration","content_type","updated_at_db","purchase_flag","subscription_flag","tv_everywhere_flag","free_web_flag","netflix_purchase_flag","netflix_purchase_id","netflix_subscription_flag","netflix_subscription_id","netflix_tv_everywhere_flag","netflix_tv_everywhere_id","netflix_free_web_flag","netflix_free_web_id","Rovi_id","multiple_id_flag_netflix","netflix_link_proejctx_ids","comment"]
	writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
	writer.writeheader()
	total=0
	x=0
	y=0
	z=0
	for mm in range(start,end,1000):
            query1=mytable.aggregate([{"$skip":mm},{"$limit":1000},{"$match":{"$and":[{"show_type":{"$in":['SE','MO']}},{"$and":[{"purchase_web_sources": {"$ne": {"$in":[[],{"$type":None}]}}},{"subscription_web_sources": {"$ne":{"$in":[[],{"$type":None}]}}},{"tv_everywhere_web_sources": {"$ne":{"$in":[[],{"$type":None}]}}},{"free_web_sources":{"$ne":{"$in":[[],{"$type":None}]}}}]}]}},{"$sort":{"updated_at":-1}},{"$project":{"gb_id":1,"_id":0,"show_type":1,"updated_at":1,"purchase_web_sources":1,"subscription_web_sources":1,"tv_everywhere_web_sources":1,"free_web_sources":1,"series_title":1,"original_title":1,"alternate_titles":1,"season_number":1,"episode_number":1,"release_year":1,"first_aired":1,"duration":1,"content_type":1,"title":1}}],allowDiskUse=True)
            for ii in query1:
                gb_id=ii.get("gb_id")
		show_type=ii.get("show_type")
		updated_at_db=ii.get("updated_at")
		print(ii.get("title"),ii.get("gb_id"),ii.get("show_type"))
		title=unidecode.unidecode(ii.get("title"))
		original_title=ii.get("original_title")
		original_title=unidecode.unidecode(original_title)
		try:
		   series_title=unicode(str(ii.get("series_title")),'utf-8')
		   series_title=unidecode.unidecode(series_title)
		except (AttributeError,UnicodeEncodeError):
		   series_title=ii.get("series_title")
		   series_title=unidecode.unidecode(series_title)
		alternate_titles=ii.get("alternate_titles")
		season_number=ii.get("season_number")
		episode_number=ii.get("episode_number")
		release_year=ii.get("release_year")
		air_date=ii.get("first_aired")
		duration=ii.get("duration")
		content_type=ii.get("content_type")
		description=unicode(str(ii.get("overview")),'utf-8')
		description=unidecode.unidecode(description)

#		import pdb;pdb.set_trace() 
		netflix_free_web_id_arr=[]
		netflix_subscription_id_arr=[]
		netflix_purchase_id_arr=[]
		netflix_tv_everywhere_id_arr=[]
		arr_px_netflix=[]
		arr_gb_netflix=[]
		arr_rovi_netflix=[]
		sec_arr_netflix=[]
		multiple_id_flag_netflix='False'
		netflix_purchase_flag=''
		netflix_subscription_flag=''
		netflix_tv_everywhere_flag=''
		netflix_free_web_flag=''
		comment1=''
		comment2=''
		total=total+1
		print (total,name)
		purchase=ii.get("purchase_web_sources")
		if ii.get("purchase_web_sources")!=[] and ii.get("purchase_web_sources") is not None:
		    purchase_flag='True'
		    purchase=ii.get("purchase_web_sources")  
		    for aa in purchase:
		        if "netflix" in aa.get("source"):
		            netflix_purchase_flag='True'                 
		            netflix_link1=(bb.get("link")) .encode('utf-8')
		            netflix_purchase_id=re.findall("\w+.*?",netflix_link1)[-1:][0]
		            netflix_purchase_id_arr.append(netflix_purchasee_id)
		            break
		        else:
		            netflix_purchase_flag='False' 
		else:
		    purchase_flag='False'
		    netflix_purchase_flag='False'
		if ii.get("subscription_web_sources")!=[] and ii.get("subscription_web_sources") is not None:
         	    subscription_flag='True'
		    subscription=ii.get("subscription_web_sources")
		    for bb in subscription:
		        if "netflix" in bb.get("source"):
		            netflix_subscription_flag='True'
		            netflix_link2=(bb.get("link")) .encode('utf-8')
		            netflix_subscription_id=re.findall("\w+.*?",netflix_link2)[-1:][0]
		            netflix_subscription_id_arr.append(netflix_subscription_id)   
		            break
		        else:
		            netflix_subscription_flag='False' 
		else:
		    subscription_flag='False'
		    netflix_subscription_flag='False'
		if ii.get("tv_everywhere_web_sources")!=[] and ii.get("tv_everywhere_web_sources") is not None:
		    tv_everywhere_flag='True'
		    tv_everywhere=ii.get("tv_everywhere_web_sources")
		    for bb in tv_everywhere:
		        if "netflix" in bb.get("source"):
		            netflix_tv_everywhere_flag='True'
		            netflix_link3=(bb.get("link")) .encode('utf-8')
		            netflix_tv_everywhere_id=re.findall("\w+.*?",netflix_link3)[-1:][0]
		            netflix_tv_everywhere_id_arr.append(netflix_tv_everywhere_id)
		            break
		        else:
		            netflix_tv_everywhere_flag='False'
		else:
		    tv_everywhere_flag='False'
		    netflix_tv_everywhere_flag='False'              
		if ii.get("free_web_sources")!=[] and ii.get("free_web_sources") is not None:
         	    free_web_flag='True'
		    free_web=ii.get("free_web_sources")
		    for bb in free_web:
		        if "netflix" in bb.get("source"):
		            netflix_free_web_flag='True'
		            netflix_link4=(bb.get("link")).encode('utf-8')
		            netflix_free_web_id=re.findall("\w+.*?",netflix_link4)[-1:][0]
		            netflix_free_web_id_arr.append(netflix_free_web_id)
		            break
		        else:
		            netflix_free_web_flag='False'
		else:
		    free_web_flag='False'          
		    netflix_free_web_flag='False'
		if (purchase_flag=='False' or netflix_purchase_flag=='False') and (free_web_flag=='False' or netflix_free_web_flag=='False') and (tv_everywhere_flag=='False' or netflix_tv_everywhere_flag=='False') and (subscription_flag=='False' or netflix_subscription_flag=='False'):
		    y=y+1   
                    multiple_id_flag_netflix='Null'  
		    writer.writerow({"gb_id":gb_id,"show_type":show_type,"title":title,"description":description,"original_title":original_title,"series_title":series_title,"alternate_titles":alternate_titles,"season_number":season_number,"episode_number":episode_number,"release_year":release_year,"air_date":air_date,"duration":duration,"content_type":content_type,"updated_at_db":updated_at_db,"purchase_flag":purchase_flag,"subscription_flag":subscription_flag,"tv_everywhere_flag":tv_everywhere_flag,"free_web_flag":free_web_flag,"netflix_purchase_flag":netflix_purchase_flag,"netflix_purchase_id":netflix_purchase_id_arr,"netflix_subscription_flag":netflix_subscription_flag,"netflix_subscription_id":netflix_subscription_id_arr,"netflix_tv_everywhere_flag":netflix_tv_everywhere_flag,"netflix_tv_everywhere_id":netflix_tv_everywhere_id_arr,"netflix_free_web_flag":netflix_free_web_flag,"netflix_free_web_id":netflix_free_web_id_arr,"multiple_id_flag_netflix":multiple_id_flag_netflix,"netflix_link_proejctx_ids":'',"comment":'No links in Database'})
		#import pdb;pdb.set_trace()
		try:
		    if netflix_purchase_flag=='True' or netflix_subscription_flag=='True' or netflix_tv_everywhere_flag=='True' or netflix_free_web_flag=='True':
		        netflix_arr=netflix_purchase_id_arr+netflix_subscription_id_arr+netflix_tv_everywhere_id_arr+netflix_free_web_id_arr
		        for jj in netflix_arr:
		            if netflix_arr.count(jj)>1:
		                netflix_arr.remove(jj)
		            if jj in netflix_arr:
		                if netflix_arr.count(jj)>1:
		                    netflix_arr.remove(jj)
		        url_link="http://34.231.212.186:81/projectx/%s/netflixusa/ottprojectx" %netflix_arr[0]
		        response_link=urllib2.Request(url_link)
		        response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
		        resp_link=urllib2.urlopen(response_link)
		        data_link=resp_link.read()
		        data_resp_link=json.loads(data_link)
		        if show_type=='MO':
		            if data_resp_link:
		                for tt in data_resp_link:
		                    if tt.get("sub_type")=="MO" and tt.get("type")=='Program' and tt.get("data_source")=='GuideBox':
		                        arr_px_netflix.append(tt.get("projectx_id"))
		                        arr_gb_netflix.append(tt.get("source_id"))
		                    if tt.get("type")=='Program' and tt.get("data_source")=='Rovi':
		                        arr_px_netflix.append(tt.get("projectx_id"))
		                        arr_rovi_netflix.append(tt.get("source_id"))
		            else:
		                comment2='netflix link is not ingested'   
		            for aa in arr_px_netflix:
		                if arr_px_netflix.count(aa)>1:
		                    arr_px_netflix.remove(aa)
		            for aa in arr_rovi_netflix:
        	                if arr_rovi_netflix.count(aa)>1:
		                    arr_rovi_netflix.remove(aa)
		            for aa in arr_gb_netflix:
		                if arr_gb_netflix.count(aa)>1:
		                    arr_gb_netflix.remove(aa)
		            for jj in arr_px_netflix:
		                sec_arr_netflix.append(jj)
		            if len(sec_arr_netflix)>1:
                                px_link="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true" %'{}'.format(",".join([str(i) for i in sec_arr_netflix]))
                                response_link=urllib2.Request(px_link)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link1=json.loads(data_link)
                                for hh in data_resp_link1:
                                    if hh.get("variant_parent_id") is not None and hh.get("title_parent_id") is not None:
                                        if hh.get("variant_parent_id")==hh.get("id"):
                                            comment2='multiple projectx ids found in netflix movie'
                                            multiple_id_flag_netflix='True'
                                        else:
                                            try:
                                                sec_arr_netflix.remove(hh.get("variant_parent_id"))
                                            except ValueError:
                                                print("variant id not in the list") 
                                            #sec_arr_amazon.remove(hh.get("id"))
                                            if len(sec_arr_netflix)>1:
                                                comment2='multiple projectx ids found in netflix movie'
                                                multiple_id_flag_netflix='True'
                                            else:
                                                multiple_id_flag_netflix='False'
                                                comment2=''
                                    elif hh.get("variant_parent_id") is not None and hh.get("title_parent_id") is None:
                                        if hh.get("variant_parent_id")==hh.get("id"):
                                            comment2='multiple projectx ids found in netflix movie'
                                            multiple_id_flag_netflix='True'
                                        else:
                                            try:
                                                sec_arr_netflix.remove(hh.get("variant_parent_id"))
                                            except ValueError:
                                                print("variant id not in the list") 
                                            #sec_arr_hulu_se.remove(hh.get("id"))
                                            if len(sec_arr_netflix)>1:
                                                comment2='multiple projectx ids found in netflix movie'
                                                multiple_id_flag_netflix='True'
                                            else:
                                                multiple_id_flag_netflix='False'
                                                comment2=''
                                    elif hh.get("variant_parent_id") is None and (hh.get("title_parent_id") is not None and hh.get("title_parent_id")!=0):
                                        if hh.get("title_parent_id")==hh.get("id"):
                                            if len(sec_arr_netflix)>1:
                                                multiple_id_flag_netflix='True'
                                                comment2='multiple projectx ids found in netflix movie'
                                            else:
                                                multiple_id_flag_netflix='False'
                                                comment2=''  
                                        else:
                                            if len(sec_arr_netflix)>1:
                                                multiple_id_flag_netflix='True'
                                                comment2='multiple projectx ids found in netflix movie'
                                            else:
                                                multiple_id_flag_netflix='False'
                                                comment2=''
                                    elif (hh.get("variant_parent_id") is None or hh.get("variant_parent_id")==0) and (hh.get("title_parent_id") is None or hh.get("title_parent_id")==0):
                                        if len(sec_arr_netflix)>1:
                                            multiple_id_flag_netflix='True'
                                            comment2='multiple projectx ids found in netflix movie' 
                                        else:
                                            multiple_id_flag_netflix='False'
                                            comment2=''
		            else:
		                multiple_id_flag_netflix='False'
		        if show_type=='SE':
		            if data_resp_link:
		                for tt in data_resp_link:
		                    if tt.get("sub_type")=="SE" and tt.get("type")=='Program' and tt.get("data_source")=='GuideBox':
		                        arr_px_netflix.append(tt.get("projectx_id"))
		                        arr_gb_netflix.append(tt.get("source_id"))
		                    if tt.get("type")=='Program' and tt.get("data_source")=='Rovi':
		                        arr_px_netflix.append(tt.get("projectx_id"))
		                        arr_rovi_netflix.append(tt.get("source_id"))
		            else:
		                comment2='netflix link is not ingested'
		            for aa in arr_px_netflix:
		                if arr_px_netflix.count(aa)>1:
		                    arr_px_netflix.remove(aa)
		            for jj in arr_px_netflix:
		                sec_arr_netflix.append(jj)
		            for aa in arr_rovi_netflix:
		                if arr_rovi_netflix.count(aa)>1:
		                    arr_rovi_netflix.remove(aa)
		            for aa in arr_gb_netflix:
		                if arr_gb_netflix.count(aa)>1:
		                    arr_gb_netflix.remove(aa)
		            if len(sec_arr_netflix)>1:
                                px_link="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs?ids=%s&ott=true" %'{}'.format(",".join([str(i) for i in sec_arr_netflix]))
                                response_link=urllib2.Request(px_link)
                                response_link.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                                resp_link=urllib2.urlopen(response_link)
                                data_link=resp_link.read()
                                data_resp_link1=json.loads(data_link)
                                for hh in data_resp_link1:
                                    if hh.get("variant_parent_id") is not None and hh.get("title_parent_id") is not None:
                                        if hh.get("variant_parent_id")==hh.get("id"):
                                            comment2='multiple projectx ids found in netflix episode'
                                            multiple_id_flag_netflix='True'
                                        else:
                                            try:
                                                sec_arr_netflix.remove(hh.get("variant_parent_id"))
                                            except ValueError:
                                                print("variant id not in the list")
                                            if len(sec_arr_netflix)>1:
                                                comment2='multiple projectx ids found in netflix episode'
                                                multiple_id_flag_netflix='True'
                                            else:
                                                multiple_id_flag_netflix='False'
                                                comment2=''
                                    elif hh.get("variant_parent_id") is not None and hh.get("title_parent_id") is None:
                                        if hh.get("variant_parent_id")==hh.get("id"):
                                            comment2='multiple projectx ids found in netflix episode'
                                            multiple_id_flag_netflix='True'
                                        else:
                                            try:
                                                sec_arr_netflix.remove(hh.get("variant_parent_id"))
                                            except ValueError:
                                                print("variant id not in the list")
                                            if len(sec_arr_netflix)>1:
                                                comment2='multiple projectx ids found in netflix episode'
                                                multiple_id_flag_netflix='True'
                                            else:
                                                multiple_id_flag_netflix='False'
                                                comment2=''
                                    elif hh.get("variant_parent_id") is None and (hh.get("title_parent_id") is not None and hh.get("title_parent_id")!=0):
                                        if hh.get("title_parent_id")==hh.get("id"):
                                            if len(sec_arr_netflix)>1:
                                                multiple_id_flag_netflix='True'
                                                comment2='multiple projectx ids found in netflix movie'
                                            else:
                                                multiple_id_flag_netflix='False'
                                                comment2=''
                                        else:
                                            if len(sec_arr_netflix)>1:
                                                multiple_id_flag_netflix='True'
                                                comment2='multiple projectx ids found in netflix movie'
                                            else:
                                                multiple_id_flag_netflix='False'
                                                comment2=''
                                    elif (hh.get("variant_parent_id") is None or hh.get("variant_parent_id")==0) and (hh.get("title_parent_id") is None or hh.get("title_parent_id")==0):
                                        if len(sec_arr_netflix)>1:
                                            multiple_id_flag_netflix='True'
                                            comment2='multiple projectx ids found in netflix episode'
                                        else:
                                            multiple_id_flag_netflix='False'
                                            comment2=''
		            else:
		                multiple_id_flag_netflix='False'  
 #                   import pdb;pdb.set_trace()  
		    if multiple_id_flag_netflix=='True' :
                        x=x+1
		        writer.writerow({"gb_id":gb_id,"show_type":show_type,"title":title,"description":description,"original_title":original_title,"series_title":series_title,"alternate_titles":alternate_titles,"season_number":season_number,"episode_number":episode_number,"release_year":release_year,"air_date":air_date,"duration":duration,"content_type":content_type,"updated_at_db":updated_at_db,"purchase_flag":purchase_flag,"subscription_flag":subscription_flag,"tv_everywhere_flag":tv_everywhere_flag,"free_web_flag":free_web_flag,"netflix_purchase_flag":netflix_purchase_flag,"netflix_purchase_id":netflix_purchase_id_arr,"netflix_subscription_flag":netflix_subscription_flag,"netflix_subscription_id":netflix_subscription_id_arr,"netflix_tv_everywhere_flag":netflix_tv_everywhere_flag,"netflix_tv_everywhere_id":netflix_tv_everywhere_id_arr,"netflix_free_web_flag":netflix_free_web_flag,"netflix_free_web_id":netflix_free_web_id_arr,"Rovi_id":{"netflix":arr_rovi_netflix},"multiple_id_flag_netflix":multiple_id_flag_netflix,"netflix_link_proejctx_ids":sec_arr_netflix,"comment":comment2})

		    if multiple_id_flag_netflix=='False':
                        z=z+1
		        writer.writerow({"gb_id":gb_id,"show_type":show_type,"title":title,"description":description,"original_title":original_title,"series_title":series_title,"alternate_titles":alternate_titles,"season_number":season_number,"episode_number":episode_number,"release_year":release_year,"air_date":air_date,"duration":duration,"content_type":content_type,"updated_at_db":updated_at_db,"purchase_flag":purchase_flag,"subscription_flag":subscription_flag,"tv_everywhere_flag":tv_everywhere_flag,"free_web_flag":free_web_flag,"netflix_purchase_flag":netflix_purchase_flag,"netflix_purchase_id":netflix_purchase_id_arr,"netflix_subscription_flag":netflix_subscription_flag,"netflix_subscription_id":netflix_subscription_id_arr,"netflix_tv_everywhere_flag":netflix_tv_everywhere_flag,"netflix_tv_everywhere_id":netflix_tv_everywhere_id_arr,"netflix_free_web_flag":netflix_free_web_flag,"netflix_free_web_id":netflix_free_web_id_arr,"multiple_id_flag_netflix":multiple_id_flag_netflix,"netflix_link_proejctx_ids":sec_arr_netflix,"comment":comment2})
                    print ({"total":total, "netflix_link not ingested": y, "multiple ids": x, "single ids": z, "thread_name":name})

		except httplib.BadStatusLine:
		    print ("exception caught httplib.BadStatusLine................................................................................")
		    continue
		except urllib2.HTTPError:
		    print ("exception caught HTTPError............................................................................................")
		    continue
		except socket.error:
		    print ("exception caught SocketError..........................................................................................")
		    continue
		except URLError:
		    print ("exception caught URLError.............................................................................................")
		    continue    
    connection.close()
    print ({"total":total, "netflix_link not ingested": y, "multiple ids": x, "single ids": z })
  
                 

t1=threading.Thread(target=id_search_amazon,args=(0,"starting thread-1",100000,1))
t1.start()
t2 =threading.Thread(target=id_search_amazon,args=(100001,'starting thread-2',200000,2))
t2.start()
t3 = threading.Thread(target=id_search_amazon,args=(200001,'starting thread-3',300000,3))
t3.start()
t4 = threading.Thread(target=id_search_amazon,args=(300001,'starting thread-4',400000,4))
t4.start()
t5 = threading.Thread(target=id_search_amazon,args=(400001,'starting thread-5',500000,5))
t5.start()
t6 = threading.Thread(target=id_search_amazon,args=(500001,'starting thread-6',600000,6))
t6.start()
t7 =threading.Thread(target=id_search_amazon,args=(600001,'starting thread-7',700000,7))
t7.start()
t8 = threading.Thread(target=id_search_amazon,args=(700001,'starting thread-8',900000,8))
t8.start()


"""writer: Saayan"""


import urllib2
import json
import os
from pprint import pprint
import urllib
import sys
import csv
import pymysql
import MySQLdb
import threading
from collections import Counter
from urllib2 import HTTPError
import datetime

class api(threading.Thread):

    def __init__(self,start_id,name,end_id,id):
	threading.Thread.__init__(self)
	print "starting" +''+ self.name
	result_sheet='/validation_images%d.csv'%id
        if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
    	csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
        w=open(os.getcwd()+result_sheet,"wa")
        with w as mycsvfile:
            fieldnames = ["projectx_id","rovi_id","Projectx_image_urls","Preprod_images_urls","missing_image_url","Extra_link_exists","comment","Result"]
            writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
            writer.writeheader()
	    Pass=0
            for j in range(start_id,end_id):
	        while True:
		    try:
                        print (j)
                	url="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d" %j
                	print url
                	response=urllib2.Request(url)
                	response.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                	resp=urllib2.urlopen(response)
                	data=resp.read()
                	data_resp=json.loads(data)
                	rovi_id=[]
                	gb_id=[]
	        	url1="http://34.231.212.186:81/projectx/%d/mapping"%j
	        	response1=urllib2.Request(url1)
                	response1.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                	resp1=urllib2.urlopen(response1)
                	data1=resp1.read()
               	 	data_resp1=json.loads(data1)
	        	Url_px=[]
	        	arr=[]
	        	m=0
	        	n=0
	        	Url_preprod=[]
	        	arr1=[]	
	        	comp1=[]
	        	comp2=[]
			print("Pass count",Pass)
	        	for i in data_resp1:
	            	    if i["data_source"]=='Rovi':
                        	rovi_id.append(i["source_id"].encode())
	                	url2="http://preprod.caavo.com/programs/%d?&ott=true"%eval(rovi_id[0])
		        	#import pdb;pdb.set_trace()
		        	response2=urllib2.Request(url2)
                        	response2.add_header('Authorization','Token token=709d07cc85ab679bdc9c9a7124cead72f6465301c142bfdd2b7e5c2852c75b89')
                        	resp2=urllib2.urlopen(response2)
                        	data2=resp2.read()
                        	data_resp2=json.loads(data2)
		        	for i in data_resp:
		            	    for x in i["images"]:
			        	if 'rovi' in x.get("url"):
		                    	    Url_px.append((x.get("url")).encode("utf-8"))
				    	    if len(x.get("tags"))>1:
				        	Url_px.append(map(str,[i.encode() for i in x.get("tags")]))
				            else:
			                        Url_px.append((x.get("tags")[0]).encode("utf-8"))
			            	    Url_px.append((x.get("aspect_ratio")).encode("utf-8"))
			            	    Url_px.append((x.get("orientation")).encode("utf-8"))
		                    	    arr.insert(m,Url_px)
			            	    m=m+1
				            Url_px=[]
		        	for i in data_resp2:
		  	     	    if i["images"]==[] and arr==[]:
			                Pass=Pass+1
                                	print("preprod and PX have no images")
					break
			    	    if arr and i["images"]==[]:
			                writer.writerow({"projectx_id":str(j),"rovi_id":str(rovi_id[0]),"Projectx_image_urls":arr,"missing_image_url":[],"Extra_link_exists":[],"Preprod_images_urls":'',"comment":"preprod have no images","Result":"null"}) 	 
			            else:
				        #import pdb;pdb.set_trace()
			                for x in i["images"]:
			 	            if 'rovi' in (x.get("url")).encode("utf-8"):
				                Url_preprod.append((x.get("url")).encode("utf-8"))
					        if len(x.get("tags"))>1:
					            Url_preprod.append(map(str,[i.encode() for i in x.get("tags")]))
					        else:
				                    Url_preprod.append((x.get("tags")[0]).encode("utf-8"))
				            	Url_preprod.append((x.get("aspect_ratio")).encode("utf-8"))
				            	Url_preprod.append((x.get("orientation")).encode("utf-8"))
				            	arr1.insert(n,Url_preprod)
				            	n=n+1
				            	Url_preprod=[]
                    	            	preprod=Counter(map(str,[i for i in arr1]))
				    	if arr:
			                    px=Counter(map(str,[i for i in arr]))
			            	    e=preprod-px
			            	    f=px-preprod
			            	    if list(e.elements()):
				                comp1.append(list(e.elements()))
			            	    if list(f.elements()):
                                                comp2.append(list(f.elements()))	
				
                                    	    if comp1==[] and comp2==[]:
                                                writer.writerow({"projectx_id":str(j),"rovi_id":str(rovi_id[0]),"Projectx_image_urls":[],"missing_image_url":comp1,"Extra_link_exists":comp2,"Preprod_images_urls":'',"comment":"Images are correct populated in API","Result":"Pass"})
	       		                    if comp1==[] and comp2:
                   	                        writer.writerow({"projectx_id":str(j),"rovi_id":str(rovi_id[0]),"Projectx_image_urls":arr,"missing_image_url":'',"Extra_link_exists":comp2,"Preprod_images_urls":'',"comment":"Additional Images populated in API compare to preprod","Result":"Fail"})
	                                    if comp1 and comp2==[]:
                                                writer.writerow({"projectx_id":str(j),"rovi_id":str(rovi_id[0]),"Projectx_image_urls":arr,"missing_image_url":comp1,"Extra_link_exists":'',"Preprod_images_urls":'',"comment":"Missing Images in API compare to preprod","Result":"Fail"})
	                                    if comp1 and comp2:
                                                writer.writerow({"projectx_id":str(j),"rovi_id":str(rovi_id[0]),"Projectx_image_urls":arr,"missing_image_url":comp1,"Extra_link_exists":comp2,"Preprod_images_urls":'',"comment":"Missing and Additional Images in API compare to preprod","Result":"Fail"})
				        else:
				            writer.writerow({"projectx_id":str(j),"rovi_id":str(rovi_id[0]),"Projectx_image_urls":arr,"missing_image_url":comp1,"Extra_link_exists":comp2,"Preprod_images_urls":arr1,"comment":"images present in preprod API compare to projectx","Result":"Fail"})
	            except HTTPError:
			print("caught exception")
			continue
		    except urllib2.URLError:
			print("caught exception")
                        continue
		    break


        print datetime.datetime.now()
t1=api(1,'thread-1',50001,1) 
t1.start()   
t2=api(50001,'thread-2',100001,2)
t2.start()
t3=api(100001,'thread-3',150001,3)
t3.start()
t4=api(150001,'thread-4',200000,4)
t4.start()
t5=api(200001,'thread-5',250001,5)
t5.start()
t6=api(250001,'thread-6',300001,6)
t6.start()


t1.join()
t2.join()
t3.join()
t4.join()
t5.join()
t6.join()

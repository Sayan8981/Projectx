
"""writer : Saayan """

import threading
import pymongo
from pprint import pprint
import sys
import os
import csv
import pymysql
import collections
from pprint import pprint
import MySQLdb
import re
import collections
from collections import Counter
import datetime

class ott(threading.Thread):

    def __init__(self,start,name,end,id):

        threading.Thread.__init__(self)
        print self.name
        print("Checking Ott_contents of SE for Guidebox to Projectx .........")
        connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
        mydb=connection["ozone"]
        mytable=mydb["guidebox_program_details"]

        conn1=pymysql.connect(user="projectx",passwd="projectx",host="52.91.200.186",db="projectx",port=3370)
        cur1=conn1.cursor()
        result_sheet='/validation_ott_links_content_gb_SE%d.csv'%id
        if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
        w=open(os.getcwd()+result_sheet,"wa")
        with w as mycsvfile:
            fieldnames = ["Gb_sm_id","Gb_id","projectx_id_gb","Missing ott_contents","Result of missing ott_conents","Comment for missing ott_contents","Additional/Duplicates ott_contents","Result of additional/duplicates ott_contents","Comment for additional/duplicates ott_contents","Result for Ott_link population"]
            writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
            writer.writeheader()

            total=0
            t=0
            h=0
            v=0
            m=0
            w=0
            GB_id=[]
	    for aa in range(start,end,1000):
		try:
	            res1=mytable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"$and":[{"show_type":"SE"},{"$or":[{"purchase_web_sources": {"$exists": True, "$ne": []}},{"subscription_web_sources": {"$exists": True, "$ne": []}},{"tv_everywhere_web_sources": {"$exists": True, "$ne": []}},{"purchase_android_sources": {"$exists": True, "$ne": []}},{"subscription_android_sources": {"$exists": True, "$ne": []}},{"tv_everywhere_android_sources": {"$exists": True, "$ne": []}},{"purchase_ios_sources": {"$exists": True, "$ne": []}},{"subscription_ios_sources": {"$exists": True, "$ne": []}},{"tv_everywhere_ios_sources": {"$exists": True, "$ne": []}}]}]}},{"project":{"gb_id":1,"show_id":1,"_id":0}}])
		    print "start_id: "+str(start)
                    for i in res1:
		        print("total count of GB ID: ", total ,"Not ingested Gb_id :", w, "Ott_link pass: ",t, "Duplicates/Additinal ott_contents :",h, "missing ott_contentsand additional/duplicates ott_contents :", v, "missing ott_contents: ", m, "Total fail : ",h+v+m, "Total pass: ", t)
                        gb_id=i.get("gb_id")
		        print "start_id: "+str(start)
		        print "end_id: "+str(end)
                        gb_sm_id=i.get("show_id")
                        print gb_id 
                        print("Gb series id :",gb_sm_id)
                        query="SELECT projectx_id FROM projectx.ProjectxMaps where data_source='GuideBox' and source_id=%s and type='Program' and sub_type='SE';"   
                        cur1.execute(query,(str(gb_id),))
                        res=cur1.fetchall()
                        if list(res)==[]:
                            w=w+1
		            GB_id.append([{str(gb_id):str(gb_sm_id)}])
	                else:
			    total=total+1
                            projectx_id_gb=str((res[0])[0])	           
                            j=0
                            arr=[]
        	            arr1=[]                                                  #26327,46363,11179,154374,13,7
          	            arr2=[]
        	            arr3=[]
        	            arr4=[]
        	            arr5=[]
        	            arr6=[]
        	            arr7=[]
        	            arr8=[]
        	            arr9=[]
        	            arr10=[]
       	   	            arr11=[]
         	            arr20=[]
        	            arr21=[]
                            arr23=[]
                            array1=[]
                            array2=[]
                            array3=[]
                            array4=[]
                            array5=[]
                            array8=[]
                            array9=[]
                            array10=[]
                            array11=[]
                            array12=[]
                            array14=[]
                            array15=[]
                            array16=[]
                            array17=[]
                            print("gb_id : ", gb_id)
                            print("gb_sm_id: ", gb_sm_id)
                            res3=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"purchase_android_sources.link":1,"purchase_android_sources.formats.format":1,"purchase_android_sources.formats.price":1,"purchase_android_sources.formats.type":1,"purchase_android_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            for y in res3:
                                a=y.get("purchase_android_sources")
                                if a:
                                    for x in a:
                                        b=(x.get("link")).encode("utf-8")
                                        if 'vuduapp' in b:
                                            a1=re.findall("\w+.*?", b)[-1:][0]
                                            arr.insert(j,a1)
                                        j=j+1    
                                        if 'hbo.hbonow' in b or 'hbonow://asset' in b:
                                            a2=re.findall("\w+.*?", b)[-1:][0]
                                            arr.insert(j,a2)
                                        j=j+2
                                        if 'play.google' in b:
                                            try:
				      	        a3=re.findall("\w+-.*?\-(\-\w+.*?)", b)[-1:][0]  #### 
                                                arr.insert(j,a3)
                                            except IndexError:
                                                a3=re.findall("\w+.*?\-(\w+.*?\w+)", b)[-1:][0]  #### 
                                                arr.insert(j,a3)
                                        j=j+3
                                        if 'amazon.com' in b:
                                            a4=re.findall("\w+.*?", b)[-1:][0]
                                            arr.insert(j,a4)
                                else:
                                    print("No android sources")

                            dict1_={}              
                            k=0
                            g=0
		            res3=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"purchase_android_sources.link":1,"purchase_android_sources.formats.format":1,"purchase_android_sources.formats.price":1,"purchase_android_sources.formats.type":1,"purchase_android_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            for i in res3:
                                c=i.get("purchase_android_sources")
                                if c:
                                    for x in c:
                                        source=str((x.get("source")).encode("utf-8"))
			                if 'amazon_prime' in source or 'amazon_buy' in source:
                                            source='amazon'
                                        if 'netflix' in source:
                                            source='netflixusa'
			                if source=='hbo':
                                            source='HBO'
                                        if source=='hbo_now':
                                            source='hbogo'
			                if source=='google_play':
                                            source='googleplay'
			  	        if source=='youtube_purchase':
                                            source='youtube'
                                        formats=x.get("formats")
			   	        LinK=(x.get("link")).encode("utf-8")
                                        if formats:
                                            l=0
                                            for z in formats:
                                                arr1.insert(j,(z.get("format")).encode("utf-8"))
                                                j=j+1
                                            for z in formats:
                        	                array1.insert(j,(z.get("price")).encode("utf-8"))
                        	                j=j+1
                    	  	            for z in formats:
                        	                array2.insert(j,(z.get("type")).capitalize().encode("utf-8"))
                                                j=j+1
                                                for i in range(0,len(array2)):
                                                    if array2[i]=='Purchase':
                                                        array2[i]='Buy'
                                            if len(arr1)==4 or len(arr1)==2 or len(arr1)==6:
                                                try:
                                                    for y in range(g,len(arr1)):
                                                        arr2.insert(y,arr[k])
                                                        arr3.insert(y,arr2[y])
                                                except IndexError:
                                                    print("out of bound")
                    
                                            if len(arr1)==3 or len(arr1)==1 or len(arr1)==5:
                                                try:
                                                    for y in range(g,len(arr1)):
                                                        arr2.insert(y,arr[k])
                                                        arr3.insert(y,arr2[y])
                                                except IndexError:
                                                    print("out of bound") 
                    		            try:
                        	                for x in arr3:
                            		            array3.insert(g,x)
                            		            array3.insert(g+1,arr1[l])
                            		            array3.insert(g+2,array1[l])
                            		            array3.insert(g+3,array2[l])
                            		            array5.insert(l,array3)
                            	  	            l=l+1
                            		            array3=[]
                    	  	            except IndexError:
                      	                        print("out of bound")
                    		            dict1_[source]=array5

                    	  	            array1=[]
                   	 	            array2=[]
                                            array5=[]
				            array3=[]
                                            try:
                                                k=k+1
                                                for x in range(len(arr1)-1,-1,-1):
                                                    arr1.pop(x)
                                            except IndexError:
                                                print("list got empty")
                                        else:
                                            z=set(arr)-set(arr3)
                                            if z:
                                                d=list(z)
                                                for x in range(0,len(d)):
					            if d[x] in LinK:
                                                        dict1_[source]=[[d[x],'None','0.00','None']]
                                                k=k+1
                                            else:
                                                dict1_[source]=[[arr[k],'None','0.00','None']]
                                                k=k+1
                                else:
                                    print("no format available for purchase android")
                
                            res5=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"tv_everywhere_android_sources.link":1,"tv_everywhere_android_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
        	            for i in res5:
		                a=i.get("tv_everywhere_android_sources")
                                if a:
                                    for x in a:
                                        c=(x.get("source")).encode("utf-8")
      		                        b=(x.get("link")).encode("utf-8")
			                if 'amazon_prime' in c or 'amazon_buy' in c:
                                            c='amazon'
                                        if 'netflix' in c:
                                            c='netflixusa'
			                if c=='hbo':
                                            c='HBO'
                                        if c=='hbo_now':
                                            c='hbogo'
			                if c=='google_play':
                                            c='googleplay'
			  	        if c=='starz_tveverywhere':
				            c='starz'
                                        if "hbogo://deeplink/MO.MO" in b or 'hbonow://asset' in b:
  		                            a1=re.findall("\w+.*?", b)[-1:][0]
				            try:
                                                dict1_[c].append([a1,'None','0.00','None'])
                                                arr3.insert(j,a1)
                                            except KeyError:
                                                dict1_[c]=[[a1,'None','0.00','None']]
                                                arr3.insert(j,a1)
                                        j=j+1
     	                                if "starz://play" in b:
                                            a2=re.findall("\w+.*?", b)[-1:][0]
				            try:
                                                dict1_[c].append([a2,'None','0.00','None'])
                                                arr3.insert(j,a2)
                                            except KeyError:
                                                dict1_[c]=[[a2,'None','0.00','None']]
                                                arr3.insert(j,a2)
                                        j=j+2
                                        if 'http://www.showtimeanytime.com/#' in b:
                                            a3=re.findall("\d+\w+", b)[0]
				            try:
                                                dict1_[c].append([a3,'None','0.00','None'])
                                                arr3.insert(j,a3)
                                            except KeyError:
                                                dict1_[c]=[[a3,'None','0.00','None']]
                                                arr3.insert(j,a3)
                                else:
                                    print("no link for tv android")
                            res6=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"subscription_android_sources.link":1,"subscription_android_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            for i in res6:
                                a=i.get("subscription_android_sources")
                                if a:
                                    for x in a:
                                        c=(x.get("source")).encode("utf-8")
                                        if 'amazon_prime' in c or 'amazon_buy' in c:
                                            c='amazon'
 			                if 'netflix' in c:
                                            c='netflixusa'
			                if c=='hbo':
                                            c='HBO'
                                        if c=='hbo_now':
                                            c='hbogo'
			                if c=='google_play':
                                            c='googleplay'
			   	        if c=='hulu_plus':
				            c='hulu'
                                        b=(x.get("link")).encode("utf-8")
                                        if 'vuduapp' in b:
                                            a1=re.findall("\w+.*?", b)[-1:][0]
				            try:
                                                dict1_[c].append([a1,'None','0.00','None'])
                                                arr3.insert(j,a1)
                                            except KeyError:
                                                dict1_[c]=[[a1,'None','0.00','None']]
                                                arr3.insert(j,a1)
                                        j=j+1
                                        if 'hbo.hbonow' in b or 'hbonow://asset' in b:
				            try:
				                a2=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
				                try:
                                       	            dict1_[c].append([a2,'None','0.00','None'])
                                                    arr3.insert(j,a2)
                                    	        except KeyError:
                                                    dict1_[c]=[[a2,'None','0.00','None']]
                                       	            arr3.insert(j,a2)
				            except IndexError:
				                try:
                                                    a2=re.findall("\w+.*?",b)
                                                    a3=':'.join(map(str, [a2[i] for i in range(10,14)]))
				                    try:
				    	                dict1_[c].append([a3,'None','0.00','None'])
                                                        arr3.insert(j,a3)
				                    except KeyError:
                                                        dict1_[c]=[[a3,'None','0.00','None']]
                                                        arr3.insert(j,a3)
                                                except IndexError:
                                    	            a2=re.findall("\w+.*?", b)[-1:][0]
				    	            try:
				                        dict1_[c].append([a2,'None','0.00','None'])
                                                        arr3.insert(j,a2)
				    	            except KeyError:
                                                        dict1_[c]=[[a2,'None','0.00','None']]
                                                        arr3.insert(j,a2)
                                        j=j+2
                                        try:
                                            if 'play.google' in b:
                                                a3=re.findall("\w+-\w+.*?",b)[0]
				                try:
				                    dict1_[c].append([a3,'None','0.00','None'])
                                                    arr3.insert(j,a3)
				                except KeyError:
                                                    dict1_[c]=[[a3,'None','0.00','None']]
                                                    arr3.insert(j,a3)
                                        except TypeError:
                                            a3=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict1_[c]=[[a3,'None','0.00','None']]
                                                arr3.insert(j,a3)
				            except KeyError:
                                                dict1_[c]=[[a3,'None','0.00','None']]
                                                arr3.insert(j,a3)
                                        j=j+3
                                        if 'amazon.com' in b:
                                            a4=re.findall("\w+.*?", b)[-1:][0]
			 	            try:
				                dict1_[c].append([a4,'None','0.00','None'])
                                                arr3.insert(j,a4)
			 	            except KeyError:
                                                dict1_[c]=[[a4,'None','0.00','None']]
                                                arr3.insert(j,a4)
                                        j=j+4
                                        if 'nflx://www.netflix.com/Browse?' in b:
                                            #a5=re.findall("\w+.*?",b)[-1:][0]
				            a5=re.findall("\d+.*?",b)[-1:][0]
			 	            try:
				                dict1_[c].append([a5,'None','0.00','None'])
                                                arr3.insert(j,a5)
				            except KeyError:
                                                dict1_[c]=[[a5,'None','0.00','None']]
				                arr3.insert(j,a5)
                                        j=j+5
                                        if 'hulu://w' in b or 'android-app://com.hulu.plus/https/www.hulu.com' in b:
                                            a6=re.findall("\w+.*?",b)[-1:][0]
				            try:
				                dict1_[c].append([a6,'None','0.00','None'])
                                                arr3.insert(j,a6)
				            except KeyError:
				                dict1_[c]=[[a6,'None','0.00','None']]
                                                arr3.insert(j,a6)
    	                        else:
                                    print("no link for subscription android")
                            f=dict1_.keys()
                            dict2_={}
                            source=[]
                            query22="SELECT distinct(name) FROM projectx.ProgramOtts inner join projectx.OttSources on OttSources.id=ProgramOtts.ott_source where data_source='Guidebox' and platform='android' and program_id=%s"
                            cur1.execute(query22,(projectx_id_gb,))
                            res22=cur1.fetchall()
                            if res22:
                                for x in res22:
                                    for i in x:
                                        source.append(i)
                                for x in f: 
                                    if x in source:
                                        arr20=[]
                                        query21="SELECT source_id,format,price,purchase_type FROM projectx.ProgramOtts inner join projectx.OttSources on OttSources.id=ProgramOtts.ott_source where data_source='Guidebox' and platform='android' and program_id=%s and name=%s" 

                                        cur1.execute(query21,(projectx_id_gb,x,))
                                        res21=cur1.fetchall()
                                        if res21:
                                            for y in res21:
                                                try:
                                                    arr20.append(map(str,[i for i in list(y)]))
                                                    dict2_[x]=arr20
                                                except TypeError:
                                                    print("null values")
                                        else:
                                            dict2_[x]=list(list(res21))
                                    else:
                                        print("source is not present in ottsource table", x)
                            comp=[]
                            comp3=[]
                            dict3_=dict()
		            dict20_=dict()
                            for key in dict1_.keys():
                                gb=Counter(map(str,[i for i in dict1_.get(key)]))
		                try:
                	            px=Counter(map(str,[i for i in dict2_.get(key)]))
 #               	            import pdb;pdb.set_trace()
                                    e=px-gb
                                    if list(e.elements()):
                                        comp.append(list(e.elements()))
                                        dict3_.setdefault(key, [])
                                        dict3_[key].append(list(e.elements()))
			            j=gb-px
                                    if list(j.elements()):
                                        comp3.append(list(j.elements()))
                                        dict20_.setdefault(key, [])
                                        dict20_[key].append(list(j.elements()))
		                except TypeError:
                                    px=Counter()
                                    e=px-gb
                                    if list(e.elements()):
                                        comp.append(list(e.elements()))
                                        dict3_.setdefault(key, [])
                                        dict3_[key].append(list(e.elements()))
                                    j=gb-px
                                    if list(j.elements()):
                                        comp3.append(list(j.elements()))
                                        dict20_.setdefault(key, [])
                                        dict20_[key].append(list(j.elements()))

                            j=0
                            res7=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"purchase_ios_sources.link":1,"purchase_ios_sources.formats.format":1,"purchase_ios_sources.formats.price":1,"purchase_ios_sources.formats.type":1,"purchase_ios_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            print res7
                            for i in res7:
                                a=i.get("purchase_ios_sources")
                                if a:
                                    for x in a:
                                        b=(x.get("link")).encode("utf-8")            
                                        try:
                                            if 'itunes.apple.com/' in b:
					        a1=re.findall("\d+", b)[-2:-1][0]
                                                arr4.insert(j,a1)
                                        except IndexError:
	                	            try:
					        a1=re.findall("\d+", b)[1:-2][0]
                                                arr4.insert(j,a1)
				            except IndexError:
                                                a1=re.findall("\d+.*?",b)[0]
                                                arr4.insert(j,a1)    
                                        j=j+1
                                        #if "http://click.linksynergy.com/fs-bin/click?id=Pz66xbzAbFo&subid=&offerid=251672.1&type=10&tmpid=9417&RD_PARM1=https%3A%2F%2Fwww.vudu.com%2Fcontent" in b:
                                        #   a2=re.findall("\d+", b)[-1:][0]
                                      #     arr4.insert(j,a2)
                                      #j=j+2
                                        if "http://click.linksynergy.com/fs-bin" in b:
                                            a3=re.findall("\d+", b)[-1:][0]
                                            arr4.insert(j,a3)
                                        j=j+2
                                        if 'amazon.com' in b:
                                            a3=re.findall("\w+.*?", b)[-1:][0]
                                            arr4.insert(j,a3)
                                else:
                                    print("no ios source for android")
            
                            dict4_={}
                            k=0 
                            g=0
		            res7=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"purchase_ios_sources.link":1,"purchase_ios_sources.formats.format":1,"purchase_ios_sources.formats.price":1,"purchase_ios_sources.formats.type":1,"purchase_ios_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            for i in res7:
                                c=i.get("purchase_ios_sources")
                                if c:
                                    for x in c:
                                        source=str((x.get("source")).encode("utf-8"))
                                        formats=x.get("formats")
			                if 'amazon_prime' in source or 'amazon_buy' in source:
                                            source='amazon'
                                        if 'netflix' in source:
                                            source='netflixusa'
			                if source=='hbo':
                                            source='HBO'
                                        if source=='hbo_now':
                                            source='hbogo'
			                if source=='google_play':
                                            source='googleplay'
			   	        if source=='youtube_purchase':
                                            source='youtube'
				        #if source=='verizon_on_demand':
                                        #   source='verizon'
                                        l=0
				        Link=(x.get("link")).encode("utf-8")
                                        if formats:
                                            for z in formats:
                                                arr5.insert(j,(z.get("format")).encode("utf-8"))
                                                j=j+1
                                            print arr5
                                            for z in formats:
                                                array8.insert(j,(z.get("price")).encode("utf-8"))
                                                j=j+1
                                            for z in formats:
                                                array9.insert(j,(z.get("type")).capitalize().encode("utf-8"))
                                                j=j+1
                                                for i in range(0,len(array9)):
                                                    if array9[i]=='Purchase':
                                                        array9[i]='Buy'
                                            if len(arr5)==4 or len(arr5)==2 or len(arr5)==6:
                                                try:
                                                    for y in range(g,len(arr5)):
                                                        arr6.insert(y,arr4[k])
                                                        arr10.insert(y,arr6[y])
                                                except IndexError:
                                                    print("out of bound")

                                            if len(arr5)==3 or len(arr5)==1 or len(arr5)==5:
                                                try:
                                                    for y in range(g,len(arr5)):
                                                        arr6.insert(y,arr4[k])
                                                        arr10.insert(y,arr6[y])
                                                except IndexError:
                                                    print("out of bound")
                                            try:
                                                for x in arr10:
                                                    array10.insert(g,x)
                                                    array10.insert(g+1,arr5[l])
                                                    array10.insert(g+2,array8[l])
                                                    array10.insert(g+3,array9[l])
                                                    array11.insert(l,array10)
                                                    l=l+1
                                                    array10=[]
                                            except IndexError:
                                                print("out of bound")
                                            dict4_[source]=array11
                                            array8=[]
                                            array9=[]
                                            array11=[]
				            array10=[]
                                            try:
                                                k=k+1
                                                for x in range(len(arr5)-1,-1,-1):
                                                    arr5.pop(x)
                                            except IndexError:
                                                print("list got empty")
                                        else:
                                            z=set(arr4)-set(arr10)
                                            if z:
                                                d=list(z)
                                                for x in range(0,len(d)):
					            if d[x] in Link:
                                                        dict4_[source]=[[d[x],'None','0.00','None']]
                                                k=k+1
                                            else:
                                                dict4_[source]=[[arr4[k],'None','0.00','None']]
                                                k=k+1
                                else:
                                    print("no format available for purchase ios")
                                 
	         
	                    res9=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"subscription_ios_sources.link":1,"subscription_ios_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            for i in res9:
                                a=i.get("subscription_ios_sources")
                                if a:
                                    for x in a:
                                        b=(x.get("link")).encode("utf-8")
                                        c=(x.get("source")).encode("utf-8")
                                        if 'amazon_prime' in c or 'amazon_buy' in c:
				            c='amazon'
			                if 'netflix' in c:
                                            c='netflixusa'
			                if c=='hbo':
                                            c='HBO'
                                        if c=='hbo_now':
                                            c='hbogo'
			                if c=='google_play':
                                            c='googleplay'
				        if c=='hulu_plus':
				            c='hulu'
				    #if c=='verizon_on_demand':
                                     #   c='verizon'
			   	        if c=='showtime_subscription':
				            c='showtime'
                                        if 'vuduapp' in b:
                                            a1=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict4_[c].append([a1,'None','0.00','None'])
                                                arr10.insert(j,a1)
				            except KeyError:
                                                dict4_[c]=[[a1,'None','0.00','None']]
                                                arr10.insert(j,a1)
                                        j=j+1
                                        if 'hbo.hbonow' in b or 'hbonow://asset' in b:
                                            a2=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict4_[c].append([a2,'None','0.00','None'])
                                                arr10.insert(j,a2)
				            except KeyError:
                                                dict4_[c]=[[a2,'None','0.00','None']]
                                                arr10.insert(j,a2)
                                        j=j+2
                                        try:
                                            if 'play.google' in b:
                                                a3=re.findall("\w+-\w+.*?",b)[0]
				                try:
					            dict4_[c].append([a3,'None','0.00','None'])
                                                    arr10.insert(j,a3)
				                except KeyError:
                                                    dict4_[c]=[[a3,'None','0.00','None']]
                                                    arr10.insert(j,a3)
                                        except TypeError:
                                            a3=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict4_[c].append([a3,'None','0.00','None'])
                                                arr10.insert(j,a3)
				            except KeyError:
                                                dict4_[c]=[[a3,'None','0.00','None']]
                                                arr10.insert(j,a3)
                                        j=j+3
                                        if 'amazon.com' in b:
                                            a4=re.findall("\w+.*?", b)[-1:][0]
			 	            try:
				                dict4_[c].append([a4,'None','0.00','None'])
                                                arr10.insert(j,a4)
				            except KeyError:
                                                dict4_[c]=[[a4,'None','0.00','None']]
                                                arr10.insert(j,a4)
                                        j=j+4
                                        if 'nflx://www.netflix.com/watch' in b:
                                            a5=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict4_[c].append([a5,'None','0.00','None'])
                                                arr10.insert(j,a5)
				            except KeyError:
                                                dict4_[c]=[[a5,'None','0.00','None']]
                                                arr10.insert(j,a5)
                                        j=j+5
                                        if 'showtime://PAGE' in b:
                                            a6=re.findall("\d+\w+",b)[0]
			 	            try:
				                dict4_[c].append([a6,'None','0.00','None'])
                                                arr10.insert(j,a6)
				            except KeyError:
 				                dict4_[c]=[[a6,'None','0.00','None']]
                                                arr10.insert(j,a6)
                                        j=j+6
                                        if 'hulu://w' in b:
                                            a7=re.findall("\w+.*?",b)[-1:][0]
				            try:
				                dict4_[c].append([a7,'None','0.00','None'])
                                                arr10.insert(j,a7)
				            except KeyError:
                                                dict4_[c]=[[a7,'None','0.00','None']]
                                                arr10.insert(j,a7)
                                        j=j+7
            		                if 'aiv://aiv/play?asin' in b:
                                            a8=re.findall("\w+\d+\w+",b)[0]
				            try:
				                dict4_[c].append([a8,'None','0.00','None'])
                                                arr10.insert(j,a8)
				            except KeyError:
			    	                dict4_[c]=[[a8,'None','0.00','None']]
                                                arr10.insert(j,a8)
                                else:
                                    print("no link for subscription ios")


                            res10=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"tv_everywhere_ios_sources.link":1,"tv_everywhere_ios_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            for i in res10:
                                a=i.get("tv_everywhere_ios_sources")
                                if a:
                                    for x in a:
                                        b=(x.get("link")).encode("utf-8")
                                        c=(x.get("source")).encode("utf-8")
			                if 'amazon_prime' in c or 'amazon_buy' in c:
                                            c='amazon'
                                        if 'netflix' in c:
                                            c='netflixusa'
			                if c=='hbo':
                                            c='HBO'
                                        if c=='hbo_now':
                                            c='hbogo'
			   	        if c=='starz_tveverywhere':
				            c='starz'
                                        if "hbogo://deeplink/MO.MO" in b or 'hbonow://asset' in b:
                                            a1=re.findall("\w+.*?", b)[-1:][0]
			    	            try:
				                dict4_[c].append([a1,'None','0.00','None'])
                                                arr10.insert(j,a1)
				            except KeyError:
                                                dict4_[c]=[[a1,'None','0.00','None']]
                                                arr10.insert(j,a1)
                                        j=j+1
                                        if "hbogo://urn:hbo:tile" in b:
				            try:
				 	        a2=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
					        try:
                                                    dict4_[c].append([a2,'None','0.00','None'])
                                                    arr10.insert(j,a2)
                                                except KeyError:
                                                    dict4_[c]=[[a2,'None','0.00','None']]
                                                    arr10.insert(j,a2)
				            except IndexError:
				                a2=re.findall("\w+.*?",b)
                                                a3=':'.join(map(str, [a2[i] for i in range(1,5)]))  
				                try:
				                    dict4_[c].append([a3,'None','0.00','None'])
                                                    arr10.insert(j,a3)
				                except KeyError:
                                                    dict4_[c]=[[a3,'None','0.00','None']] 
                                                    arr10.insert(j,a3)
                                        j=j+2
                   	                if "starz://play" in b:
                                            a3=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict4_[c].append([a3,'None','0.00','None'])
                                                arr10.insert(j,a3)
				            except KeyError:
                                                dict4_[c]=[[a3,'None','0.00','None']]
                       		                arr10.insert(j,a3)
                                        j=j+3
                                        if 'shoany://PAGE/' in b:
                                            a4=re.findall("\d+\w+", b)[0]
				            try:
				                dict4_[c].append([a4,'None','0.00','None'])
                                                arr10.insert(j,a4)
			  	            except KeyError:
                                                dict4_[c]=[[a4,'None','0.00','None']]
                                                arr10.insert(j,a4)
            	                else:
                                    print("no link for tv ios")
               
                            f=dict4_.keys()
                            dict5_={}
                            source=[]
                            query23="SELECT distinct(name) FROM projectx.ProgramOtts inner join projectx.OttSources on OttSources.id=ProgramOtts.ott_source where data_source='Guidebox' and platform='ios' and program_id=%s"
                            cur1.execute(query23,(projectx_id_gb,))
                            res23=cur1.fetchall()
                            if res23:
                                for x in res23:
                                    for i in x:
                                        source.append(i)
                                for x in f:
                                    if x in source:
                                        arr21=[]
                                        query24="SELECT source_id,format,price,purchase_type FROM projectx.ProgramOtts inner join projectx.OttSources on OttSources.id=ProgramOtts.ott_source where data_source='Guidebox' and platform='ios' and program_id=%s and name=%s"

                                        cur1.execute(query24,(projectx_id_gb,x,))
                                        res24=cur1.fetchall()
                                        if res24:
                                            for y in res24:
                                                try:
                                                    arr21.append(map(str,[i for i in list(y)]))
                                                    dict5_[x]=arr21
                                                except TypeError:
                                                    print("null values")
                                        else:
                                            dict5_[x]=list(list(res24))
                                    else:
                                        print("not link available")
                            comp1=[]
                            comp4=[]
                            dict6_=dict()
		            dict21_=dict()
                            for key in dict4_.keys():
               	                gb1=Counter(map(str,[i for i in dict4_.get(key)]))
		                try:
               		            px1=Counter(map(str,[i for i in dict5_.get(key)]))
               		            f=px1-gb1
                                    if list(f.elements()):
                                        comp1.append(list(f.elements()))
                                        dict6_.setdefault(key, [])
                                        dict6_[key].append(list(f.elements()))
                                    k=gb1-px1
                                    if list(k.elements()):
                                        comp4.append(list(k.elements()))
                                        dict21_.setdefault(key, [])
                                        dict21_[key].append(list(k.elements()))
                                except TypeError:
                                    px1=Counter()
                                    f=px1-gb1
                                    if list(f.elements()):
                                        comp1.append(list(f.elements()))
                                        dict6_.setdefault(key, [])
                                        dict6_[key].append(list(f.elements()))
                                    k=gb1-px1
                                    if list(k.elements()):
                                        comp4.append(list(k.elements()))
                                        dict21_.setdefault(key, [])
                                        dict21_[key].append(list(k.elements()))


 

                            j=0  
                            res11=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"purchase_web_sources.link":1,"purchase_web_sources.formats.format":1,"purchase_web_sources.formats.price":1,"purchase_web_sources.formats.type":1,"purchase_web_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)  
                            for y in res11:
	   	                a=y.get("purchase_web_sources")
                                if a:
                                    for x in a:
                                        b=(x.get("link")).encode("utf-8")
                                        if 'vuduapp' in b:
                                            a1=re.findall("\w+.*?", b)[-1:][0]
                                            arr7.insert(j,a1)
                                        j=j+1
                                        if 'hbo.hbonow' in b or 'hbonow://asset' in b:
                                            a2=re.findall("\w+.*?", b)[-1:][0]
                                            arr7.insert(j,a2)
                                        j=j+2
                                        try:
                                            if 'play.google' in b:
						a3=re.findall("\w+-.*?\-\-\w+.*?\w+\-(\w+.*)", b)[0]
                                                arr7.insert(j,a3)
					except IndexError:
					    try:
					        a3=re.findall("\w+-.*?\-(\-\w+.*?)", b)[-1:][0]  #### 
                                                arr7.insert(j,a3)
                                            except IndexError:
						try:
						    a3=re.findall("\w+.*?\-(\w+.*?\w+\w+)", b)[1]
                                                    arr7.insert(j,a3)
						except IndexError:
			  		            a3=re.findall("\w+.*?\-(\w+.*?\w+)", b)[-1:][0]  #### 
                                                    arr7.insert(j,a3)
                                        j=j+3
                                        try:
                                            if '//itunes.apple.com/us/tv-season' in b:
					        a5=re.findall("\d+", b)[-2:-1][0]
                                                arr7.insert(j,a5)
                                        except IndexError:
				            try:
					        a5=re.findall("\d+", b)[1:-2][0]
                                                arr7.insert(j,a5)
				            except IndexError:
                                                a5=re.findall("\d+", b)[0]
                                                arr7.insert(j,a5)
                                        j=j+4
                                        if '//www.amazon.com/gp' in b:
                                            a6=re.findall("\w+\d+\w+", b)[0]
                                            arr7.insert(j,a6)
                                        j=j+5
                                        if '//click.linksynergy.com/' in b:
                                            a7=re.findall("\d+.*?", b)[-1:][0]
                                            arr7.insert(j,a7)
                                        j=j+6
                                        try:
                                            if '//www.youtube.com/w' in b:
                                                a8=re.findall("\w+-\w+.*?",b)[0] 
                                                arr7.insert(j,a8)
                                        except IndexError:
				            try:
			   		        a8=re.findall("\-\w+.*?",b)[0]
                                                arr7.insert(j,a8)
				            except IndexError:
                                                try:
                                                    a8=re.findall("\w+\d+\w+", b)[0]
                                                    arr7.insert(j,a8)
                                                except IndexError:
                                                    a8=re.findall("\w+.*?",b)[-1:][0]
                                                    arr7.insert(j,a8)
                                        j=j+7
                                        if '//www.verizon.com/' in b:
                                            a9=re.findall("\w+\d+", b)[-1:][0]   #"""***************"""
                                            arr7.insert(j,a9)
                                        j=j+8
                                        if 'https://www.paramountmovies.com' in b:
                                            try:
                                                a10=re.findall("\w+\d+" ,b)[-1:][0]
                                                arr7.insert(j,a10)
                                            except IndexError:
                                                try:
                                                    a10=re.findall("\w+\d+\w+" ,b)[-1:][0]
                                                    arr7.insert(j,a10)
                                                except IndexError:
				  		    try:
                                                        a10=re.findall("\w+" ,b)[-1:][0]
                                                        arr7.insert(j,a10)
					            except IndexError:
						        pass
                                        j=j+9
                                        if 'https://store.sonyentertainmentnetwork.com/' in b:
                                            a11=re.findall("\w+-\w+-\w+.*?",b)[0]
				            arr7.insert(j,a11)
                                        j=j+10
                                        if 'https://www.mgo.com/' in b:
                                            a12=re.findall("\w+\d+\w+",b)[0]
                                            arr7.insert(j,a12)

                                else:
                                    print("No android sources")

                            dict7_=dict()
                            k=0
                            g=0 
		            res11=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"purchase_web_sources.link":1,"purchase_web_sources.formats.format":1,"purchase_web_sources.formats.price":1,"purchase_web_sources.formats.type":1,"purchase_web_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            for i in res11:
                                c=i.get("purchase_web_sources")
                                if c:
                                    for x in c:
                                        source=str((x.get("source")).encode("utf-8"))
                    	                formats=x.get("formats")
			                if 'amazon_prime' in source or 'amazon_buy' in source:
                                            source='amazon'
                                        if 'netflix' in source:
                                            source='netflixusa'
			                if source=='hbo':
                                            source='HBO'
                                        if source=='hbo_now':
                                            source='hbogo'
			                if source=='google_play':
                                            source='googleplay'
                                        if source=='youtube_purchase':
                                            source='youtube'
				        #if source=='verizon_on_demand':
				         #   source='verizon'
			   	        link=(x.get("link")).encode("utf-8")
                    	                if formats:
                                            array16=[]
                                            l=0
                                            for z in formats:
                           	                arr8.insert(j,(z.get("format")).encode("utf-8"))
                                                j=j+1
                                            print arr8
                                            for z in formats:
                                                array14.insert(j,(z.get("price")).encode("utf-8"))
                                                j=j+1
                                            for z in formats:
                                                array15.insert(j,(z.get("type")).capitalize().encode("utf-8"))
                                                j=j+1
                                                for i in range(0,len(array15)):
                                                    if array15[i]=='Purchase':
                                                        array15[i]='Buy'

                                            if len(arr8)==4 or len(arr8)==2:
                                                try:
                                                    for y in range(g,len(arr8)):
                                                        arr9.insert(y,arr7[k])
                                                        arr11.insert(y,arr9[y])
                                                except IndexError:
                                                    print("out of bound")

                                            if len(arr8)==3 or len(arr8)==1:
                                                try:
                                                    for y in range(g,len(arr8)):
                                                        arr9.insert(y,arr7[k])
                                                        arr11.insert(y,arr9[y])
                                                except IndexError:
                                                    print("out of bound")
                                            try:
                                                for x in arr11:
                                                    array16.insert(g,x)
                                                    array16.insert(g+1,arr8[l])
                                                    array16.insert(g+2,array14[l])
                                                    array16.insert(g+3,array15[l])
                                                    array17.insert(l,array16)
                                                    array16=[]
                                                    l=l+1
                                            except IndexError:
                                                print("out of bound")
                                            dict7_[source]=array17

                                            array14=[]
                                            array15=[]
                                            array17=[]
				            array16=[]
                                            try:
                                                k=k+1
                                                for x in range(len(arr8)-1,-1,-1):
                                                    arr8.pop(x)
                                            except IndexError:
                                                print("list got empty")
                                        else:
                                            c=set(arr7)-set(arr11)
                                            if c:
                                                d=list(c)
					        for y in range(0,len(d)):
					   	    if d[y]=='':
						        d[y]='null'
                                                for x in range(0,len(d)):
					            if d[x] in link:
                                                        dict7_[source]=[[d[x],'None','0.00','None']]
                                                k=k+1
                                            else:
                                                dict7_[source]=[[arr7[k],'None','0.00','None']]
                                                k=k+1
                                else:      
                                    print("no format available purchase web")
        
                            res13=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"subscription_web_sources.link":1,"subscription_web_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                            print res13
                            for i in res13:
                                a=i.get("subscription_web_sources")
                                if a:
                                    for x in a:
                                        b=(x.get("link")).encode("utf-8")
                                        c=(x.get("source")).encode("utf-8")
			                if 'amazon_prime' in c or 'amazon_buy' in c:
				            c='amazon'
			                if 'netflix' in c:
                                            c='netflixusa'
			                if c=='hbo':
                                            c='HBO'
                                        if c=='hbo_now':
                                            c='hbogo'
			                if c=='google_play':
                                            c='googleplay'
	  		    	        if c=='hulu_plus':
				            c='hulu'
				        #if c =='verizon_on_demand':
                                         #   c='verizon'
			   	        if c=='showtime_subscription':
                                            c='showtime'
                                        if 'vuduapp' in b:
                                            a1=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict7_[c].append([a1,'None','0.00','None'])
                                                arr11.insert(j,a1)
				            except KeyError:
                                                dict7_[c]=[[a1,'None','0.00','None']]
                                                arr11.insert(j,a1)
                                        j=j+1
                                        if 'hbo.hbonow' in b or 'hbonow://asset' in b:
                                            a2=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict7_[c].append([a2,'None','0.00','None'])
                                                arr11.insert(j,a2)
				            except KeyError:
                                                dict7_[c]=[[a2,'None','0.00','None']]
                                                arr11.insert(j,a2)
                                        j=j+2
                                        try:
                                            if 'play.google' in b:
                                                a3=re.findall("\w+-\w+.*?",b)[0]
				                try:
				                    dict7_[c].append([a3,'None','0.00','None'])
                                                    arr11.insert(j,a3)
				                except KeyError:
                                                    dict7_[c]=[[a3,'None','0.00','None']]
                                                    arr11.insert(j,a3)
                                        except TypeError:
                                            a3=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict7_[c].append([a3,'None','0.00','None'])
                                                arr11.insert(j,a3)
				            except KeyError:
                                                dict7_[c]=[[a3,'None','0.00','None']]
                      	                        arr11.insert(j,a3)
                                        j=j+3
                       	                if 'aiv://aiv/play' in b:
                      		            a4=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict7_[c].append([a4,'None','0.00','None'])
                                                arr11.insert(j,a4)
			  	            except KeyError:
                                                dict7_[c]=[[a4,'None','0.00','None']]
                      	   	                arr11.insert(j,a4)
                                        j=j+4   
                    	                if '//itunes.apple.com/us/movie' in b:
                                            a5=re.findall("\d+", b)[0]
				            try:
				                dict7_[c].append([a5,'None','0.00','None'])
                                                arr11.insert(j,a5)
			  	            except KeyError:
                                                dict7_[c]=[[a5,'None','0.00','None']]
                                                arr11.insert(j,a5)
                  	                j=j+5
                                        if '//www.amazon.com/gp' in b:
                                            a6=re.findall("\w+\d+\w+", b)[0]
			  	            try:
				                dict7_[c].append([a6,'None','0.00','None'])
                                                arr11.insert(j,a6)
				            except KeyError:
                                                dict7_[c]=[[a6,'None','0.00','None']]
                       	                        arr11.insert(j,a6)
                  	                j=j+6
                   	                if '//click.linksynergy.com/' in b:
                         	            a7=re.findall("\d+", b)[-1:][0]
                                            try:
				                dict7_[c].append([a7,'None','0.00','None'])
                                                arr11.insert(j,a7)
				            except KeyError:
                                                dict7_[c]=[[a7,'None','0.00','None']]
                      		                arr11.insert(j,a7)
                                        j=j+7
                 	                if '//www.youtube.com/w' in b:
                                            a8=re.findall("\w+\d+\w+", b)[0]
				            try:
				                dict7_[c].append([a8,'None','0.00','None'])
                                                arr11.insert(j,a8)
				            except KeyError:
                                                dict7_[c]=[[a8,'None','0.00','None']]
                       	                        arr11.insert(j,a8)
             		                j=j+8
                  	                if '//www.verizon.com/Ondemand/' in b:
                       	                    a9=re.findall("\w+\d+\w+", b)[0]
				            try:
				                dict7_[c].append([a9,'None','0.00','None'])
                                                arr11.insert(j,a9)
				            except KeyError:
                                                dict7_[c]=[[a9,'None','0.00','None']]
                                                arr11.insert(j,a9)
                                        j=j+9
                                        if '//play.hbonow.com/feature/' in b:
                                            a10=re.findall("\w+.*?", b)
                                            a11=':'.join(map(str, [a10[i] for i in range(5,9)]))
				            try:
				                dict7_[c].append([a11,'None','0.00','None'])
                                                arr11.insert(j,a11)
				            except KeyError:
                                                dict7_[c]=[[a11,'None','0.00','None']]
                                                arr11.insert(j,a11)
                                        j=j+10
                                        if 'netflix.com' in b:
                                            a12=re.findall("\w+.*?",b)[-1:][0] 
				            try:
				                dict7_[c].append([a12,'None','0.00','None'])
                                                arr11.insert(j,a12)
			   	            except KeyError:
                                                dict7_[c]=[[a12,'None','0.00','None']]
                                                arr11.insert(j,a12)
                                        j=j+11
                                        if 'http://www.showtime.com/#' in b:
                                            a13=re.findall("\w+.*?",b)[-1:][0]
				            try:
				                dict7_[c].append([a13,'None','0.00','None'])
                                                arr11.insert(j,a13)
				            except KeyError:
                                                dict7_[c]=[[a13,'None','0.00','None']]
                                                arr11.insert(j,a13)
                                        j=j+12
                                        if 'http://www.hulu.com/watch' in b:
                                            a14=re.findall("\w+.*?",b)[-5:]
                                            a14='-'.join(map(str,[a14[i] for i in range(0,len(a14))]))
				            try:
				                dict7_[c].append([a14,'None','0.00','None'])
                                                arr11.insert(j,a14)
				            except KeyError:
		                                dict7_[c]=[[a14,'None','0.00','None']]
                                                arr11.insert(j,a14)
                                        j=j+13
			                if '/play.hbonow.com/episode/' in b:
				            try:
					        a15=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
				 	        try:
                                                    dict7_[c].append([a15,'None','0.00','None'])
                                                    arr11.insert(j,a15)
                                                except KeyError:
                                                    dict7_[c]=[[a15,'None','0.00','None']]
                                                    arr11.insert(j,a15)
				            except IndexError:
                                                a15=re.findall("\w+.*?.-*?", b)
                                                a16=':'.join(map(str, [a15[i] for i in range(5,len(a15))]))
				                try:
				                    dict7_[c].append([a16,'None','0.00','None'])
                                                    arr11.insert(j,a16)
				                except KeyError:
                                                    dict7_[c]=[[a16,'None','0.00','None']]
                                                    arr11.insert(j,a16)
                                else:
                                    print("no link for subscription web")
        	            res14=mytable.find({"gb_id":gb_id,"show_type":'SE'},{"tv_everywhere_web_sources.link":1,"tv_everywhere_web_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
        	            for i in res14:
		                a=i.get("tv_everywhere_web_sources")
                                if a:
                                    for x in a:
                                        b=(x.get("link")).encode("utf-8")
                                        c=(x.get("source")).encode("utf-8")
			                if 'amazon_prime' in c or 'amazon_buy' in c:
                                            c='amazon'
                                        if 'netflix' in c:
                                            c='netflixusa'
				        if c=='hbo':
                                            c='HBO'
                                        if c=='hbo_now':
                                            c='hbogo'
				        if c=='starz_tveverywhere':
				            c='starz'
                    	                if "hbogo://deeplink/" in b or 'hbonow://asset' in b: #"""****************"""
                                            a1=re.findall("\w+.*?", b)[-1:][0]
                                            try:
				                dict7_[c].append([a1,'None','0.00','None'])
                                                arr11.insert(j,a1)
			 	            except KeyError:				
                                                dict7_[c]=[[a1,'None','0.00','None']]
                        	                arr11.insert(j,a1)
                    	                j=j+1
                    	                if "starz://play" in b or "//www.starz.com/" in b:
                                            a2=re.findall("\w+.*?", b)[-1:][0]
				            try:
				                dict7_[c].append([a2,'None','0.00','None'])
                                                arr11.insert(j,a2)
				            except KeyError:
                                                dict7_[c]=[[a2,'None','0.00','None']]
                                                arr11.insert(j,a2)
                   	                j=j+2
                                        if "//play.hbogo.com/feature" in b:
				            try:
                                                a3=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                                try:
                                                    dict7_[c].append([a3,'None','0.00','None'])
                                                    arr11.insert(j,a3)
                                                except KeyError:
                                                    dict7_[c]=[[a3,'None','0.00','None']]
                                                    arr11.insert(j,a3)
                                            except IndexError:
                                                try:
                                                    a3=re.findall("\w+.*?", b)
                                                    a4=':'.join(map(str, [a3[i] for i in range(5,len(a3))]))
                                                    try:
                                                        dict7_[c].append([a4,'None','0.00','None'])
                                                        arr11.insert(j,a4)
                                                    except KeyError:
                                                        dict7_[c]=[[a4,'None','0.00','None']]
                                                        arr11.insert(j,a4)
                                                except IndexError:
                                                    a3=re.findall("\w+.*?",b)[-1:][0]
                                                    try:
                                                        dict7_[c].append([a3,'None','0.00','None'])
                                                        arr11.insert(j,a3)
                                                    except KeyError:
                                                        dict7_[c]=[[a3,'None','0.00','None']]
                                                        arr11.insert(j,a3)
                                        j=j+3
                                        if 'http://www.showtimeanytime.com/#' in b:
                                            a5=re.findall("\d+\w+", b)[0]
				            try:
			                        dict7_[c].append([a5,'None','0.00','None'])
                                                arr11.insert(j,a5)
				            except KeyError:
                                                dict7_[c]=[[a5,'None','0.00','None']]
                                                arr11.insert(j,a5)
                                        j=j+4
                                        try:
                                            if '/play.hbogo.com/episode/' in b:
					        a6=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
				    	        try:
                                                    dict7_[c].append([a6,'None','0.00','None'])
                                                    arr11.insert(j,a6)
                                                except KeyError:
                                                    dict7_[c]=[[a6,'None','0.00','None']]
                                                    arr11.insert(j,a6)
                                        except IndexError:
				            try:
				                a6=re.findall("\w+.*?.-*?", b)
                                                a7=':'.join(map(str, [a6[i] for i in range(5,len(a6))]))
				                try:
				                    dict7_[c].append([a7,'None','0.00','None'])
                                                    arr11.insert(j,a7)
				                except KeyError:
                                                    dict7_[c]=[[a7,'None','0.00','None']]
                                                    arr11.insert(j,a7) 
                                            except IndexError:
      				                a6=re.findall("\w+.*?",b)[-1:][0]
				                try:
				                    dict7_[c].append([a6,'None','0.00','None'])
                                                    arr11.insert(j,a6)
				                except KeyError:
                                                    dict7_[c]=[[a6,'None','0.00','None']]
                                                    arr11.insert(j,a6)
                                else:
                                     print("no link for tv ios") 
       
                            f=dict7_.keys()
                            dict8_=dict()
                            source=[]
                            query25="SELECT distinct(name) FROM projectx.ProgramOtts inner join projectx.OttSources on OttSources.id=ProgramOtts.ott_source where data_source='Guidebox' and platform='pc' and program_id=%s"
        	            cur1.execute(query25,(projectx_id_gb,))
        	            res25=cur1.fetchall()
                            if res25:
        	                for x in res25:
            	                    for i in x:
                                        source.append(i)
		                for x in f:
                                    if x in source:
                                        arr23=[]
                                        query26="SELECT source_id,format,price,purchase_type FROM projectx.ProgramOtts inner join projectx.OttSources on OttSources.id=ProgramOtts.ott_source where data_source='Guidebox' and platform='pc' and program_id=%s and name=%s"
                                        cur1.execute(query26,(projectx_id_gb,x,))
                                        res26=cur1.fetchall()
                                        if res26:
                                            for y in res26:
                                                try:
                                                    arr23.append(map(str,[i for i in list(y)]))
                                                    dict8_[x]=arr23
                                                except TypeError:
                                                    print("null values")
                                        else:
                                            dict8_[x]=list(list(res26))
                                    else:
				        print("Not link available")
                            comp2=[]
                            comp5=[]
                            dict9_=dict()
	 	            dict22_=dict()
                            for key in dict7_.keys():
                                gb2=Counter(map(str,[i for i in dict7_.get(key)]))
		                try:
                	            px2=Counter(map(str,[i for i in dict8_.get(key)]))
                	            g=px2-gb2
                                    if list(g.elements()):
                                        comp2.append(list(g.elements()))
                                        dict9_.setdefault(key, [])
                                        dict9_[key].append(list(g.elements()))
                                    print comp2
                                    l=gb2-px2
                                    if list(l.elements()):
                                        comp5.append(list(l.elements()))
                                        dict22_.setdefault(key, [])
                                        dict22_[key].append(list(l.elements()))
                                except TypeError:
                                    px2=Counter()
                                    g=px2-gb2
                                    if list(g.elements()):
                                        comp2.append(list(g.elements()))
                                        dict9_.setdefault(key, [])
                                        dict9_[key].append(list(g.elements()))
                                    print comp2
                                    l=gb2-px2
                                    if list(l.elements()):
                                        comp5.append(list(l.elements()))
                                        dict22_.setdefault(key, [])
                                        dict22_[key].append(list(l.elements()))
		            px_gb=[]
                            gb_px=[]
		            if comp==[] and comp1==[] and comp2==[]:                         #000
                                print("no mismatch of ott links")
                                px_gb=["Pass",'[]','No additional/duplicates ott link present']
                            if comp3==[] and comp4==[] and comp5==[]:
                                gb_px=["Pass",'[]','No missing ott link']    
          	   

                            if comp1==[] and comp2==[] and comp:#001
                                px_gb=["Fail",[{'android': dict3_}],'additional/duplicates ott_links exists']         
                            if comp4==[] and comp5==[] and comp3:
                                gb_px=["Fail",[{'android': dict20_}],'missing ott_links exists']
                    
	   	            if comp2==[] and comp==[] and comp1:#010       
                                px_gb=["Fail",[{'ios': dict6_}],'additional/duplicates ott_links exists']
 		            if comp5==[] and comp3==[] and comp4:
                                gb_px=["Fail",[{'ios': dict21_}],'missing ott_links exists']

                            if comp2 and comp==[] and comp1==[]:#100
                                px_gb=["Fail",[{'pc': dict9_}],'additional/duplicates ott_links exists']
	  	            if comp5 and comp3==[] and comp4==[]:
                                gb_px=["Fail",[{'pc': dict22_}],'missing ott_links exists']
                    
                            if comp==[] and comp1 and comp2:#011
                                px_gb=["Fail",[{'pc': dict9_},{'ios': dict6_}],'additional/duplicates ott_links exists']
		            if comp3==[] and comp4 and comp5:
                                gb_px=['Fail',[{'pc': dict22_},{'ios': dict21_}],'missing ott_links exists']
                
       	   	            if comp1==[] and comp and comp2:#101
                                px_gb=["Fail",[{'android': dict3_},{'pc': dict9_}],'additional/duplicates ott_links exists']
 		            if comp4==[] and comp3 and comp5:
		                gb_px=['Fail',[{'pc': dict22_},{'android': dict20_}],'missing ott_links exists']
         
                            if comp2==[] and comp and comp1:         #011
                                px_gb=["Fail",[{'android': dict3_},{'ios': dict6_}],'additional/duplicates ott_links exists']
                            if comp5==[] and comp3 and comp4:
                                gb_px=['Fail',[{'ios': dict21_},{'android': dict20_}],'missing ott_links exists']	    
 	 	
  	       	            if comp2 and comp and comp1:
                                px_gb=["Fail",[{'android': dict3_},{'ios': dict6_},{'pc': dict9_}],'additional/duplicates ott_links exists']
		            if comp5 and comp3 and comp4:
                                gb_px=['Fail',[{'ios': dict21_},{'android': dict20_},{'pc': dict22_}],'missing ott_links exists']
                
	     	            if gb_px[0]=='Pass' and px_gb[0]=='Pass':
                                t=t+1
                                print({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"projectx_id_gb":str(projectx_id_gb),"Missing ott_contents":gb_px[1],"Result of missing ott_conents":gb_px[0],"Comment for missing ott_contents":gb_px[2],"Additional/Duplicates ott_contents":px_gb[1],"Result of additional/duplicates ott_contents":px_gb[0],"Comment for additional/duplicates ott_contents":px_gb[2],"Result for Ott_link population":'Pass'}) 
                            if gb_px[0]=='Pass' and px_gb[0]=='Fail':
                                h=h+1
                                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"projectx_id_gb":str(projectx_id_gb),"Missing ott_contents":gb_px[1],"Result of missing ott_conents":gb_px[0],"Comment for missing ott_contents":gb_px[2],"Additional/Duplicates ott_contents":px_gb[1],"Result of additional/duplicates ott_contents":px_gb[0],"Comment for additional/duplicates ott_contents":px_gb[2],"Result for Ott_link population":'Fail'})
                            if gb_px[0]=='Fail' and px_gb[0]=='Fail':
                                v=v+1
		                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"projectx_id_gb":str(projectx_id_gb),"Missing ott_contents":gb_px[1],"Result of missing ott_conents":gb_px[0],"Comment for missing ott_contents":gb_px[2],"Additional/Duplicates ott_contents":px_gb[1],"Result of additional/duplicates ott_contents":px_gb[0],"Comment for additional/duplicates ott_contents":px_gb[2],"Result for Ott_link population":'Fail'})
		            if gb_px[0]=='Fail' and px_gb[0]=='Pass':
                                m=m+1
		                writer.writerow({"Gb_sm_id":gb_sm_id,"Gb_id":gb_id,"projectx_id_gb":str(projectx_id_gb),"Missing ott_contents":gb_px[1],"Result of missing ott_conents":gb_px[0],"Comment for missing ott_contents":gb_px[2],"Additional/Duplicates ott_contents":px_gb[1],"Result of additional/duplicates ott_contents":px_gb[0],"Comment for additional/duplicates ott_contents":px_gb[2],"Result for Ott_link population":'Fail'})

		except (AttributeError, pymongo.errors.OperationFailure):
		    print ("caught exception .............................................................................>")
		    continue

        print("total count of GB ID: ", total ,"Not ingested Gb_id :", w, "Ott_link pass: ",t, "Duplicates/Additinal ott_contents :",h, "missing ott_contentsand additional/duplicates ott_contents :", v, "missing ott_contents: ", m, "Total fail : ",h+v+m, "Total pass: ", t)
        print("gb_ids not ingested",GB_id)
        print(datetime.datetime.now())


	connection.close()



t1=ott(0,"starting thread-1",30000,1)
t1.start()
t2 =ott(30001,'starting thread-2',60000,2)
t2.start()
t3 = ott(60001,'starting thread-3',90000,3)
t3.start()
t4 = ott(90001,'starting thread-4',120000,4)
t4.start()
t5 = ott(120001,'starting thread-5',150000,5)
t5.start()
t6 = ott(150001,'starting thread-6',180000,6)
t6.start()
t7 =ott(180001,'starting thread-7',210000,7)
t7.start()
t8 = ott(210001,'starting thread-8',240000,8)
t8.start()
t9 = ott(240001,'starting thread-9',270000,9)
t9.start()
t10= ott(270001,'starting thread-10',300000,10)
t10.start()
t11=ott(300001,'starting thread-11',330000,11)
t11.start()
t12=ott(330001,'starting thread-12',360000,12)
t12.start()
t13=ott(360001,'starting thread-13',390000,13)
t13.start()
t14=ott(390001,'starting thread-14',420000,14)
t14.start()
t15=ott(420001,'starting thread-15',450000,15)
t15.start()
t16=ott(450001,'starting thread-16',480000,16)
t16.start()
t17=ott(480001,'starting thread-17',510000,17)
t17.start()
t18=ott(510001,'starting thread-18',540000,18)
t18.start()
t19=ott(540001,'starting thread-19',570000,19)
t19.start()
t20=ott(570001,'starting thread-20',600000,20)
t20.start()
t21=ott(600001,'starting thread-21',800000,21)
t21.start()





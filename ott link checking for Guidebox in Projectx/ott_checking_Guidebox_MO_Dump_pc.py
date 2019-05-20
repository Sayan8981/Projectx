
"""Writer: Saayan"""

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


def gb_ott(start,name,end,id):
    print name
    print("start",start)
    print("end",end)    
    print("Checking Ott_contents of MO for Guidebox to Projectx for pc platform only ....................")

    connection=pymongo.MongoClient("mongodb://192.168.86.10:27017/")
    mydb=connection["ozone"]
    mytable=mydb["guidebox_program_details"]


    conn1=pymysql.connect(user="projectx",passwd="projectx",host="34.201.242.195",db="projectx",port=3370)         
    cur1=conn1.cursor()


    result_sheet='/validation_ott_links_content_gb_MO%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with w as mycsvfile:
        fieldnames = ["Gb_id","projectx_id_gb","Missing ott_contents","Result of missing ott_conents","Comment for missing ott_contents","Additional/Duplicates ott_contents","Result of additional/duplicates ott_contents","Comment for additional/duplicates ott_contents","Result for Ott_link population"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
	total=0
	t=0
	v=0
	h=0
	m=0
	w=0
	GB_id=[]
        for aa in range(start,end,1000):
	    try:
	        res1=mytable.aggregate([{"$skip":aa},{"$limit":1000},{"$match":{"$and":[{"show_type":"MO"},{"$or":[{"purchase_web_sources": {"$exists": True, "$ne": []}},{"subscription_web_sources": {"$exists": True, "$ne": []}},{"tv_everywhere_web_sources": {"$exists": True, "$ne": []}}]}]}},{"$project":{"gb_id":1,"_id":0}}])
                for i in res1:
		
                    print("thread_name:",name, "total count of GB ID: ", total ,"Not ingested Gb_id :", w, "Ott_link pass: ",t, "Duplicates/Additinal ott_contents :",v, "missing ott_contentsand additional/duplicates ott_contents :", h, "missing ott_contents: ", m, "Total fail : ",v+h+m, "Total pass: ", t)
		    print("gb ids not ingested ",GB_id, len(GB_id))
                    gb_id=i.get("gb_id")
                    print gb_id       
                    query="SELECT projectx_id FROM projectx.ProjectxMaps where data_source='GuideBox' and source_id=%s and type='Program' and sub_type='MO';"
                    cur1.execute(query,(str(gb_id),))
                    res=cur1.fetchall()
                    if list(res)==[]:
                        w=w+1
                        GB_id.append(str(gb_id))
			print("gb ids not ingested ",GB_id, len(GB_id))
                    else:
			total=total+1		
                        projectx_id_gb=str((res[0])[0])
                        j=0
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
                        print("gb_id is: ", gb_id)

                        j=0  
                        res11=mytable.find({"gb_id":gb_id,"show_type":'MO'},{"purchase_web_sources.link":1,"purchase_web_sources.formats.format":1,"purchase_web_sources.formats.price":1,"purchase_web_sources.formats.type":1,"purchase_web_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
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
			  		    a3=re.findall("\w+-\w+.\w+-\w+", b)[0]
					    arr7.insert(j,a3)
				    except IndexError:
					try:
					    a3=re.findall("\w+-\w+-\w+.*?",b)[0]
					    arr7.insert(j,a3)
					except IndexError:
				            try:
						a3=re.findall("\w+-\-\w+.*?\w+", b)[0]
						arr7.insert(j,a3)
					    except IndexError:
						try:
                                                    a3=re.findall("\-\w+-\w+.*?", b)[-1:][0]
                                                    arr7.insert(j,a3)
                                                except IndexError:
			  		            try:
						        a3=re.findall("\w+-\-\w+.*?",b)[0]
						        arr7.insert(j,a3)
						    except IndexError:
						        try:
					                    a3=re.findall("\w+-\w+.*?",b)[0]
                                                            arr7.insert(j,a3)
				  	                except IndexError:
						            try:
							       a3=re.findall("\-\-\w+.*?",b)[-1:][0]
							       arr7.insert(j,a3)
						   	    except IndexError:
							        try:
                                                                    a3=re.findall("\-\w+.*?", b)[-1:][0]
                                                                    arr7.insert(j,a3)
						                except IndexError:
						  		    a3=re.findall("\w+.*?", b)[-1:][0]
                                                           	    arr7.insert(j,a3)

                                    j=j+3
                                    try:
                                        if '//itunes.apple.com/us/movie' in b:
                                            a5=re.findall("\d+", b)[0:-2][2:][1]
                                            arr7.insert(j,a5)
                                    except IndexError:
				        try:
					    a5=re.findall("\d+", b)[1:-2][1]
					    arr7.insert(j,a5)
					except IndexError:
					    try:
                                                a5=re.findall("\d+", b)[0:-2][1:2][0]
                                                arr7.insert(j,a5)
		 	  	            except IndexError:
				                a5=re.findall("\d+", b)[0]
				                arr7.insert(j,a5)	
                                    j=j+4
                                    if '//www.amazon.com/gp' in b:
                                        a6=re.findall("\w+\d+\w+", b)[0]
                                        arr7.insert(j,a6)
                                    j=j+5
                                    if "http://click.linksynergy.com/fs-bin/click?id=Pz66xbzAbFo&subid=&offerid=251672.1&type=10&tmpid=9417&RD_PARM1=https%3A%2F%2Fwww.vudu.com%2Fcontent%2Fmovies" in b:
                                        a7=re.findall("\d+", b)[-1:][0]
                                        arr7.insert(j,a7)
                                    j=j+6
                                    if "http://click.linksynergy.com/fs-bin/click?id=Pz66xbzAbFo&subid=&offerid=251672.1&type=10&tmpid=9417&RD_PARM1=http%3A%2F%2Fwww.vudu.com%2Fmovies" in b:
                                        a3=re.findall("\d+", b)[-2:][0]
                                        arr7.insert(j,a3)
                                    j=j+7
                                    try:
                                        if '//www.youtube.com/w' in b:
					    a8=re.findall("\w+-\w+.\w+-\w+", b)[0]
					    arr7.insert(j,a8)
				    except IndexError:
					try:
					    a8=re.findall("\w+-\w+-\w+.*?",b)[0]
					    arr7.insert(j,a8)
					except IndexError:
					    try:
                                                a8=re.findall("\-\w+-\w+.*?", b)[-1:][0]
                                                arr7.insert(j,a8)
                                            except IndexError:
                                                try:
					           a8=re.findall("\w+-\w+.*?",b)[0]
					           arr7.insert(j,a8)
			  		        except IndexError:
					            try:
							a8=re.findall("\w+-\-\w+.*?",b)[0]
							arr7.insert(j,a8)
						    except IndexError:
							try:
							    a8=re.findall("\-\-\w+.*?",b)[-1:][0]
							    arr7.insert(j,a8)
							except IndexError:
							    try:
                                                                a8=re.findall("\-\w+.*?",b)[-1:][0]
                                                                arr7.insert(j,a8)
					                    except IndexError:
					  		        try:
                                                                    a8=re.findall("\w+\d+\w+", b)[0]
                                                                    arr7.insert(j,a8)
							        except IndexError:
							            a8=re.findall("\w+.*?",b)[-1:][0]
                                                                    arr7.insert(j,a8)
		
                                    j=j+8
                                    if '//www.verizon.com/Ondemand/Movies/' in b:
                                        a9=re.findall("\w+\d+\w+", b)[0]
                                        arr7.insert(j,a9)
                                    j=j+9
				    if 'https://www.paramountmovies.com' in b:
				        try:
                                            a10=re.findall("\w+\d+" ,b)[-1:][0]
                                            arr7.insert(j,a10)
				        except IndexError:
					    try:
					        a10=re.findall("\w+\d+\w+" ,b)[-1:][0]
                                                arr7.insert(j,a10)
			  		    except IndexError:
					        a10=re.findall("\w+" ,b)[-1:][0]
                                                arr7.insert(j,a10)
                                    j=j+10
                                    if 'https://store.sonyentertainmentnetwork.com/' in b:
                                        a11=re.findall("\w+-\w+-\w+.*?",b)[0]
				        arr7.insert(j,a11)
                                    j=j+11
                                    if 'https://www.mgo.com/' in b:
                                        a12=re.findall("\w+\d+\w+",b)[0]
                                        arr7.insert(j,a12)

                            else:
                                print("No android sources")

                        dict7_=dict()
                        k=0
                        g=0
		        res11=mytable.find({"gb_id":gb_id,"show_type":'MO'},{"purchase_web_sources.link":1,"purchase_web_sources.formats.format":1,"purchase_web_sources.formats.price":1,"purchase_web_sources.formats.type":1,"purchase_web_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                        for i in res11:
                            c=i.get("purchase_web_sources")
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
			    	    if source=='verizon_on_demand':
                                        source='verizon'
                    	            formats=x.get("formats")
			    	    link=(x.get("link")).encode("utf-8")
                    	            if formats:
                                        array16=[]
                                        l=0
                                        for z in formats:
                           	            arr8.insert(j,(z.get("format")).encode("utf-8"))
                                            j=j+1
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


                        res13=mytable.find({"gb_id":gb_id,"show_type":'MO'},{"subscription_web_sources.link":1,"subscription_web_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
                        for i in res13:
                            a=i.get("subscription_web_sources")
                            if a:
                                for x in a:
                                    b=(x.get("link")).encode("utf-8")
                                    c=(x.get("source")).encode("utf-8")
			            if 'amazon_prime' in c or 'amazon_buy' in c:
				        c='amazon'
			            if 'netflix' in c :
                                        c='netflixusa'
			            if c=='hbo':
                                        c='HBO'
                                    if c=='hbo_now':
                                        c='hbogo'
			            if c=='google_play':
                                        c='googleplay'
				    if c=='hulu_plus':
                                        c='hulu'
			   	    if c=='verizon_on_demand':
                                        c='verizon'
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
                                            dict7_[c]=[[a3,'None','None','None']]
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
                  	            if '//www.verizon.com/Ondemand/Movies/' in b:
                       	                a9=re.findall("\w+\d+\w+", b)[0]
				        try:
                                            dict7_[c].append([a9,'None','0.00','None'])
                                            arr11.insert(j,a9)
				        except KeyError:
				            dict7_[c]=[[a9,'None','0.00','None']]
                                            arr11.insert(j,a9)
                                    j=j+8
                                    if '//play.hbonow.com/feature/' in b:
				        try:
                                            a10=re.findall("\w+\:\w+\:\w+:\w+-\w+.*?", b)[0]
                                            try:
                                                dict7_[c].append([a10,'None','0.00','None'])
                                                arr11.insert(j,a10)
                                            except KeyError:
                                                dict7_[c]=[[a10,'None','0.00','None']]
                                                arr11.insert(j,a10)
				        except IndexError:
                                            a10=re.findall("\w+.*?", b)
                                            a11=':'.join(map(str, [a10[i] for i in range(5,9)]))
				            try:
                                                dict7_[c].append([a11,'None','0.00','None'])
                                                arr11.insert(j,a11)
			   	            except KeyError:
				                dict7_[c]=[[a11,'None','0.00','None']]
                                                arr11.insert(j,a11)
                                    j=j+9
                                    if 'http://movies.netflix.com' in b:
                                        a12=re.findall("\w+.*?",b)[-1:][0] 
				        try:
                                            dict7_[c].append([a12,'None','0.00','None'])
                                            arr11.insert(j,a12)
                                        except KeyError:
				            dict7_[c]=[[a12,'None','0.00','None']]
                                            arr11.insert(j,a12)
                                    j=j+10
                                    if 'http://www.showtime.com/#' in b:
                                        a13=re.findall("\w+.*?",b)[-1:][0]
			  	        try:
                                            dict7_[c].append([a13,'None','0.00','None'])
                                            arr11.insert(j,a13)
				        except KeyError:
				            dict7_[c]=[[a13,'None','0.00','None']]
                                            arr11.insert(j,a13)
                                    j=j+11
                                    if 'http://www.hulu.com/watch' in b:
                                        a14=re.findall("\w+.*?",b)[-1:][0]
				        try:
		                            dict7_[c].append([a14,'None','0.00','None'])
                                            arr11.insert(j,a14)
				        except KeyError:
				            dict7_[c]=[[a14,'None','0.00','None']]
                                            arr11.insert(j,a14)
			            j=j+12
			            if '/play.hbonow.com/episode/' in b:
                                        a15=re.findall("\w+.*?", b)
                                        a16=':'.join(map(str, [a15[i] for i in range(5,9)]))
			   	        try:
                                            dict7_[c].append([a16,'None','0.00','None'])
                                            arr11.insert(j,a16)	
				        except KeyError:
				            dict7_[c]=[[a16,'None','0.00','None']]
                                            arr11.insert(j,a16)

                            else:
                                print("no link for subscription web")
        	        res14=mytable.find({"gb_id":gb_id,"show_type":'MO'},{"tv_everywhere_web_sources.link":1,"tv_everywhere_web_sources.source":1},no_cursor_timeout=True).sort([("updated_at",-1)]).limit(1)
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
                    	            if "hbogo://deeplink/MO.MO" in b or 'hbonow://asset' in b:
                                        a1=re.findall("\w+.*?", b)[-1:][0]
			     	        try:
				            dict7_[c].append([a1,'None','0.00','None'])
                                            arr11.insert(j,a1)
			  	        except KeyError:
                                            dict7_[c]=[[a1,'None','0.00','None']]
                        	            arr11.insert(j,a1)
                    	            j=j+1
                    	            if "starz://play" in b or "//www.starz.com/movies" in b:
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
                                                a4=':'.join(map(str, [a3[i] for i in range(5,9)]))
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
                                            a6=re.findall("\w+.*?", b)
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
                        query25="SELECT distinct(name) FROM projectx.ProgramOtts inner join projectx.OttSources on OttSources.id=ProgramOtts.ott_source where data_source='GuideBox' and platform='pc' and program_id=%s"
            	        cur1.execute(query25,(projectx_id_gb,))
        	        res25=cur1.fetchall()
                        if res25:
        	            for x in res25:
            	                for i in x:
                                    source.append(i)
		            for x in f:
                                if x in source:
                                    arr23=[]
                                    query26="SELECT source_id,format,price,purchase_type FROM projectx.ProgramOtts inner join projectx.OttSources on OttSources.id=ProgramOtts.ott_source where data_source='GuideBox' and platform='pc' and program_id=%s and name=%s and is_deleted='0'"
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
                                l=gb2-px2
                                if list(l.elements()):
                                    comp5.append(list(l.elements()))
                                    dict22_.setdefault(key, [])
                                    dict22_[key].append(list(l.elements()))
	  	        px_gb=[]
                        gb_px=[]
			if comp2==[] and comp5==[]:
                            gb_px=["Pass",'[]','No missing ott link']
			    px_gb=["pass",[],'no additional/duplicates ott_links exists']
			if comp2 and comp5==[]:
                            px_gb=["Fail",[{'pc': dict9_}],'additional/duplicates ott_links exists']
			    gb_px=["Pass",'[]','No missing ott link']
			if comp5 and comp2==[]:
                            gb_px=["Fail",[{'pc': dict22_}],'missing ott_links exists']
			    px_gb=["pass",[],'no additional/duplicates ott_links exists']
			if comp2 and comp5:
                            px_gb=["Fail",[{'pc': dict9_}],'additional/duplicates ott_links exists']
			    gb_px=['Fail',[{'pc': dict22_}],'missing ott_links exists']
               
		        if gb_px[0]== 'Pass' and px_gb[0]=='Pass':
                            t=t+1
                            print({"Gb_id":gb_id,"projectx_id_gb":str(projectx_id_gb),"Missing ott_contents":gb_px[1],"Result of missing ott_conents":gb_px[0],"Comment for missing ott_contents":gb_px[2],"Additional/Duplicates ott_contents":px_gb[1],"Result of additional/duplicates ott_contents":px_gb[0],"Comment for additional/duplicates ott_contents":px_gb[2],"Result for Ott_link population":'Pass'})
               
		        if gb_px[0]=='Pass' and px_gb[0]=='Fail':
                            v=v+1
                            writer.writerow({"Gb_id":gb_id,"projectx_id_gb":str(projectx_id_gb),"Missing ott_contents":gb_px[1],"Result of missing ott_conents":gb_px[0],"Comment for missing ott_contents":gb_px[2],"Additional/Duplicates ott_contents":px_gb[1],"Result of additional/duplicates ott_contents":px_gb[0],"Comment for additional/duplicates ott_contents":px_gb[2],"Result for Ott_link population":'Fail'})
                        if gb_px[0]=='Fail' and px_gb[0]=='Fail':
                            h=h+1
                            writer.writerow({"Gb_id":gb_id,"projectx_id_gb":str(projectx_id_gb),"Missing ott_contents":gb_px[1],"Result of missing ott_conents":gb_px[0],"Comment for missing ott_contents":gb_px[2],"Additional/Duplicates ott_contents":px_gb[1],"Result of additional/duplicates ott_contents":px_gb[0],"Comment for additional/duplicates ott_contents":px_gb[2],"Result for Ott_link population":'Fail'})
                        if gb_px[0]=='Fail' and px_gb[0]=='Pass':
                            m=m+1
                            writer.writerow({"Gb_id":gb_id,"projectx_id_gb":str(projectx_id_gb),"Missing ott_contents":gb_px[1],"Result of missing ott_conents":gb_px[0],"Comment for missing ott_contents":gb_px[2],"Additional/Duplicates ott_contents":px_gb[1],"Result of additional/duplicates ott_contents":px_gb[0],"Comment for additional/duplicates ott_contents":px_gb[2],"Result for Ott_link population":'Fail'})

            except (AttributeError, pymongo.errors.OperationFailure):
	        print("exception Caught of cursornot found................................................")
	        continue	              
    print("thread_name:", name,"total count of GB ID: ", total ,"Not ingested Gb_id :", w, "Ott_link pass: ",t, "Duplicates/Additinal ott_contents :",v, "missing ott_contentsand additional/duplicates ott_contents :", h, "missing ott_contents: ", m, "Total fail : ",v+h+m, "Total pass: ", t)
    print("gb ids not ingested ",GB_id, len(GB_id))
    print(datetime.datetime.now())

    connection.close()

t1 = threading.Thread(target=gb_ott, args=(0,'starting thread-1',10000,1))
t1.start()
t2 = threading.Thread(target=gb_ott, args=(20001,'starting thread-2',40000,2))
t2.start()
t3 = threading.Thread(target=gb_ott, args=(40001,'starting thread-3',60000,3))
t3.start()
t4 = threading.Thread(target=gb_ott, args=(60001,'starting thread-4',80000,4))
t4.start()
t5 = threading.Thread(target=gb_ott, args=(80001,'starting thread-5',100000,5))
t5.start()

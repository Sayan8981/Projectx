"""writer: Saayan"""

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
import unidecode
import threading
import pymysql.cursors
import datetime
from collections import defaultdict

def GB_credits(start_id,name,end_id,id):
    Total=0
    r=0
    s=0
    t=0
    u=0
    v=0
    print name
    arr_mo=[]
    arr_SE=[]

    connection=pymongo.MongoClient("mongodb://192.168.86.10:27017/")
    mydb=connection["ozone"]
    mytable=mydb["guidebox_program_details"]
    conn1=pymysql.connect(user="projectx",passwd="projectx",host="54.173.42.225",db="projectx",port=3370)
    cur1=conn1.cursor()
    result_sheet='/validation_credits_guidebox%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")
    with w as mycsvfile:
        fieldnames = ["GB_id","Projectx_id_GB","show_type","Missing Credits from Guidebox","Duplicates credits in projectx","Result of populated Credits","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        for aa in range(start_id,end_id,10):
            try:
                res1=mytable.aggregate([{'$skip':aa},{'$limit':10},{"$match" :{"show_type": {"$in":["MO","SE"]}}},{"$project":{"gb_id":1,"_id":0,"show_type":1,"cast":1,"directors":1,"writers":1}},{"$sort":{"updated_at":-1}}])
	        for j in res1:
		    print("gb_id",j.get("gb_id"))
		    if j.get("gb_id") is not None:
			if j.get("show_type")=='MO':
			    arr_mo.append(j.get("gb_id"))
			if j.get("show_type")=='SE':
                            arr_SE.append(j.get("gb_id"))
		for g in arr_mo:
		    res2=mytable.find({"gb_id":g,"show_type":'MO'},{"_id":0,"cast":1,"directors":1,"show_type":1}).sort([("updated_at",-1)]).limit(1)
		    for i in res2:
			arr_mo=[]
                        Total=Total+1
                        arr_cast=[]
                        arr_director=[]
                        arr_gb_cast=[]
                        arr_px_cast=[]
                        arr_PX_cast=[]
                        GB_comp=[]
		        if i.get("cast") ==[] and i.get("directors") ==[]: #and i.get("writers") is None:#000
		            u=u+1
		            print({"GB_id":str(g),"Projectx_id_GB":'',"Missing Credits from Guidebox":'',"Duplicates credits in projectx":'',"Result of populated Credits":'',"Comment":'No credits in Guidebox DB'})
 		        if i.get("cast") and i.get("directors")==[]: #and i.get("writers") is None:#001
		            for j in i.get("cast"):
                                v1=unidecode.unidecode(j.get("name"))
                                v2=unidecode.unidecode(j.get("character_name"))
                                if v2=="Himself":
                                    v2=''
                                arr_cast.append(['Actor',v1.lower(),v2.lower()])
                                arr_gb_cast.extend(arr_cast)
			   
                            query3="select projectx_id from projectx.ProjectxMaps where source_id=%s and type='Program' and data_source='GuideBox' and sub_type='MO';"
                            cur1.execute(query3,(str(g)))
                            res3=cur1.fetchall()
                            if list(res3)==[]:
                                r=r+1
                            else:
                                for j in res3:
                                    for k in j:
                                        query4="SELECT name,lower(fullName),lower(part_name) FROM projectx.ProgramCredit inner join projectx.Credits on ProgramCredit.credit_id=Credits.id inner join projectx.CreditTypes on CreditTypes.id=ProgramCredit.credit_type where program_id=%s;"
                                        cur1.execute(query4,(k,))
                                        res4=cur1.fetchall()
		                        if res4:
                                            for l in res4:
                                                arr_px_cast.append(map(str,[x for x in list(l)]))
                                            for w in arr_px_cast:
                                                w[1]=unicode(w[1], "utf-8")
                                                w[1]=unidecode.unidecode(w[1])
                                                w[2]=unicode(w[2], "utf-8")
                                                w[2]=unidecode.unidecode(w[2])
                                                arr_PX_cast.append(w)
                                            if arr_PX_cast and arr_gb_cast:
                                                GB=Counter(map(str,[y for y in arr_gb_cast]))
                                                Px=Counter(map(str,[y for y in arr_PX_cast]))
                                                GB_px=GB-Px
                                                if list(GB_px.elements()):
                                                    GB_comp.append(list(GB_px.elements()))
                                                if GB_comp==[]:
                                                    t =t+1
                                                if GB_comp:
		     	                            for x in GB_comp:
                                                        for xx in range(0,len(x)):
                                                            for yy in arr_PX_cast:
                                                                if yy[0]==(eval(x[xx]))[0]=='Actor' or yy[0]==(eval(x[xx]))[0]=='Director':
                                                                    if (eval(x[xx]))[1] in yy:
                                                                        if x:
                                                                            x[xx]="['None']"
                                                    for jj in range(len(GB_comp[0])-1,-1,-1):
                                                        if (GB_comp[0])[jj]=="['None']":
                                                                (GB_comp[0]).pop(jj)
                                                    if GB_comp!=[[]]:
                                                        s=s+1
                                                        writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits mismatch fouund in projectx compare to Guidebox DB'})
					            else:
					                t =t+1
				        else:
				   	    v=v+1
					    writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits missing found in projectx compare to Guidebox DB'})
                        if i.get("cast")==[] and i.get("directors"): #and i.get("writers") is None:#010
                            for k in i.get("directors"):
                                d1=unidecode.unidecode(k.get("name"))
                                arr_director.append(['Director',d1.lower(),''])
                                arr_gb_cast.extend(arr_director)
                            query3="select projectx_id from projectx.ProjectxMaps where source_id=%s and type='Program' and data_source='GuideBox' and sub_type='MO';"
                            cur1.execute(query3,(str(g)))
                            res3=cur1.fetchall()
                            if list(res3)==[]:
                                r=r+1
                            else:
                                for j in res3:
                                    for k in j:
                                        query4="SELECT name,lower(fullName),lower(part_name) FROM projectx.ProgramCredit inner join projectx.Credits on ProgramCredit.credit_id=Credits.id inner join projectx.CreditTypes on CreditTypes.id=ProgramCredit.credit_type where program_id=%s;"
                                        cur1.execute(query4,(k,))
                                        res4=cur1.fetchall()
		                        if res4:
                                            for l in res4:
                                                arr_px_cast.append(map(str,[x for x in list(l)]))
                                            for w in arr_px_cast:
                                                w[1]=unicode(w[1], "utf-8")
                                                w[1]=unidecode.unidecode(w[1])
                                                w[2]=unicode(w[2], "utf-8")
                                                w[2]=unidecode.unidecode(w[2])
                                                arr_PX_cast.append(w)
                                            if arr_PX_cast and arr_gb_cast:
                                                GB=Counter(map(str,[y for y in arr_gb_cast]))
                                                Px=Counter(map(str,[y for y in arr_PX_cast]))
                                                GB_px=GB-Px
                                                if list(GB_px.elements()):
                                                    GB_comp.append(list(GB_px.elements()))
                                                if GB_comp==[]:
                                                    t =t+1
                                                if GB_comp:
		          	                    for x in GB_comp:
                                                        for xx in range(0,len(x)):
                                                            for yy in arr_PX_cast:
                                                                if yy[0]==(eval(x[xx]))[0]=='Actor' or yy[0]==(eval(x[xx]))[0]=='Director':
                                                                    if (eval(x[xx]))[1] in yy:
                                                                        if x:
                                                                            x[xx]="['None']"
                                                    for jj in range(len(GB_comp[0])-1,-1,-1):
                                                        if (GB_comp[0])[jj]=="['None']":
                                                                (GB_comp[0]).pop(jj)
                                                    if GB_comp!=[[]]:
                                                        s=s+1
                                                        writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits mismatch found in projectx compare to Guidebox DB'})
                                                    else:
                                                        t =t+1
				        else:
				  	    v=v+1	
					    writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits missing found in projectx compare to Guidebox DB'})

 	                if i.get("cast") and i.get("directors"): #and i.get("writers") is None:#110
	    	            for j in i.get("cast"):
                                v1=unidecode.unidecode(j.get("name"))
                                v2=unidecode.unidecode(j.get("character_name"))
                                if v2=="Himself":
                                    v2=''
                                arr_cast.append(['Actor',v1.lower(),v2.lower()])
                            for k in i.get("directors"):
                                d1=unidecode.unidecode(k.get("name"))
                                arr_director.append(['Director',d1.lower(),''])
                                arr_gb_cast.extend(arr_cast)
                                arr_gb_cast.extend(arr_director)
                            query3="select projectx_id from projectx.ProjectxMaps where source_id=%s and type='Program' and data_source='GuideBox' and sub_type='MO';"
                            cur1.execute(query3,(str(g)))
                            res3=cur1.fetchall()
                            if list(res3)==[]:
                                r=r+1
                            else:
                                for j in res3:
                                    for k in j:
                                        query4="SELECT name,lower(fullName),lower(part_name) FROM projectx.ProgramCredit inner join projectx.Credits on ProgramCredit.credit_id=Credits.id inner join projectx.CreditTypes on CreditTypes.id=ProgramCredit.credit_type where program_id=%s;"
                                        cur1.execute(query4,(k,))
                                        res4=cur1.fetchall()
				        if res4:
                                            for l in res4:
                                                arr_px_cast.append(map(str,[x for x in list(l)]))
                                            for w in arr_px_cast:
                                                w[1]=unicode(w[1], "utf-8")
                                                w[1]=unidecode.unidecode(w[1])
                                                w[2]=unicode(w[2], "utf-8")
                                                w[2]=unidecode.unidecode(w[2])
                                                arr_PX_cast.append(w)
		                            if arr_PX_cast and arr_gb_cast:
                                                GB=Counter(map(str,[y for y in arr_gb_cast]))
                                                Px=Counter(map(str,[y for y in arr_PX_cast]))
                                                GB_px=GB-Px
                                                if list(GB_px.elements()):
                                                    GB_comp.append(list(GB_px.elements()))
                                                if GB_comp==[]:
                                                    t =t+1
                                                if GB_comp:
			        	            for x in GB_comp:
				                        for xx in range(0,len(x)):
                                                            for yy in arr_PX_cast:
					                        if yy[0]==(eval(x[xx]))[0]=='Actor' or yy[0]==(eval(x[xx]))[0]=='Director':
					                            if (eval(x[xx]))[1] in yy:
					                                if x:
					                	            x[xx]="['None']"
				     	            for jj in range(len(GB_comp[0])-1,-1,-1):
					                if (GB_comp[0])[jj]=="['None']":
                                                                (GB_comp[0]).pop(jj)
												    
                                                    if GB_comp!=[[]]:
					                s =s+1
                                                        writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits mismatch found in projectx compare to Guidebox DB'})
					            else:
					                t =t+1
			 	        else:
                                            v=v+1
				            writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits missing found in projectx compare to Guidebox DB'})

                for g in arr_SE:
                    res2=mytable.find({"gb_id":g,"show_type":'SE'},{"_id":0,"cast":1,"directors":1,"show_type":1}).sort([("updated_at",-1)]).limit(1)
                    for i in res2:
                        arr_SE=[]
                        Total=Total+1
                        arr_cast=[]
                        arr_director=[]
                        arr_gb_cast=[]
                        arr_px_cast=[]
                        arr_PX_cast=[]
                        GB_comp=[]
                        if i.get("cast") ==[] and i.get("directors") ==[]: #and i.get("writers") is None:#000
                            u=u+1
                            print({"GB_id":str(g),"Projectx_id_GB":'',"Missing Credits from Guidebox":'',"Duplicates credits in projectx":'',"Result of populated Credits":'',"Comment":'No credits in Guidebox DB'})
                        if i.get("cast") and i.get("directors") ==[]: #and i.get("writers") is None:#001
                            for j in i.get("cast"):
                                v1=unidecode.unidecode(j.get("name"))
                                v2=unidecode.unidecode(j.get("character_name"))
                                if v2=="Himself":
                                    v2=''
                                arr_cast.append(['Actor',v1.lower(),v2.lower()])
                                arr_gb_cast.extend(arr_cast)

                            query3="select projectx_id from projectx.ProjectxMaps where source_id=%s and type='Program' and data_source='GuideBox' and sub_type='SE';"
                            cur1.execute(query3,(str(g)))
                            res3=cur1.fetchall()
                            if list(res3)==[]:
                                r=r+1
                            else:
                                for j in res3:
                                    for k in j:
                                        query4="SELECT name,lower(fullName),lower(part_name) FROM projectx.ProgramCredit inner join projectx.Credits on ProgramCredit.credit_id=Credits.id inner join projectx.CreditTypes on CreditTypes.id=ProgramCredit.credit_type where program_id=%s;"
                                        cur1.execute(query4,(k,))
                                        res4=cur1.fetchall()
                                        if res4:
                                            for l in res4:
                                                arr_px_cast.append(map(str,[x for x in list(l)]))
                                            for w in arr_px_cast:
                                                w[1]=unicode(w[1], "utf-8")
                                                w[1]=unidecode.unidecode(w[1])
                                                w[2]=unicode(w[2], "utf-8")
                                                w[2]=unidecode.unidecode(w[2])
                                                arr_PX_cast.append(w)
                                            if arr_PX_cast and arr_gb_cast:
                                                GB=Counter(map(str,[y for y in arr_gb_cast]))
                                                Px=Counter(map(str,[y for y in arr_PX_cast]))
                                                GB_px=GB-Px
                                                if list(GB_px.elements()):
                                                    GB_comp.append(list(GB_px.elements()))
                                                if GB_comp==[]:
                                                    t =t+1
                                                if GB_comp:
                                                    for x in GB_comp:
                                                        for xx in range(0,len(x)):
                                                            for yy in arr_PX_cast:
								if yy[0]==(eval(x[xx]))[0]=='Actor' or yy[0]==(eval(x[xx]))[0]=='Director':
                                                                    if (eval(x[xx]))[1] in yy:
                                                                        if x:
                                                                            x[xx]="['None']"
                                                    for jj in range(len(GB_comp[0])-1,-1,-1):
                                                        if (GB_comp[0])[jj]=="['None']":
                                                                (GB_comp[0]).pop(jj)
                                                    if GB_comp!=[[]]:
                                                        s=s+1
                                                        writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits mismatch fouund in projectx compare to Guidebox DB'})
                                                    else:
                                                        t =t+1
                                        else:
                                            v=v+1
                                            writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits missing found in projectx compare to Guidebox DB'})


                        if i.get("cast")==[] and i.get("directors"): #and i.get("writers") is None:#010
                            for k in i.get("directors"):
                                d1=unidecode.unidecode(k.get("name"))
                                arr_director.append(['Director',d1.lower(),''])
                                arr_gb_cast.extend(arr_director)
                            query3="select projectx_id from projectx.ProjectxMaps where source_id=%s and type='Program' and data_source='GuideBox' and sub_type='SE';"
                            cur1.execute(query3,(str(g)))
                            res3=cur1.fetchall()
                            if list(res3)==[]:
                                r=r+1
                                print("this GB_id not ingested in Px ")
                            else:
                                for j in res3:
                                    for k in j:
                                        query4="SELECT name,lower(fullName),lower(part_name) FROM projectx.ProgramCredit inner join projectx.Credits on ProgramCredit.credit_id=Credits.id inner join projectx.CreditTypes on CreditTypes.id=ProgramCredit.credit_type where program_id=%s;"
                                        cur1.execute(query4,(k,))
                                        res4=cur1.fetchall()
                                        if res4:
                                            for l in res4:
                                                arr_px_cast.append(map(str,[x for x in list(l)]))
                                            for w in arr_px_cast:
                                                w[1]=unicode(w[1], "utf-8")
                                                w[1]=unidecode.unidecode(w[1])
                                                w[2]=unicode(w[2], "utf-8")
                                                w[2]=unidecode.unidecode(w[2])
                                                arr_PX_cast.append(w)
                                            if arr_PX_cast and arr_gb_cast:
                                                GB=Counter(map(str,[y for y in arr_gb_cast]))
                                                Px=Counter(map(str,[y for y in arr_PX_cast]))
                                                GB_px=GB-Px
                                                if list(GB_px.elements()):
                                                    GB_comp.append(list(GB_px.elements()))
                                                if GB_comp==[]:
                                                    t =t+1
                                                if GB_comp:
                                                    for x in GB_comp:
                                                        for xx in range(0,len(x)):
                                                            for yy in arr_PX_cast:
                                                                if yy[0]==(eval(x[xx]))[0]=='Actor' or yy[0]==(eval(x[xx]))[0]=='Director':
								    if (eval(x[xx]))[1] in yy:
                                                                        if x:
                                                                            x[xx]="['None']"
                                                    for jj in range(len(GB_comp[0])-1,-1,-1):
                                                        if (GB_comp[0])[jj]=="['None']":
                                                                (GB_comp[0]).pop(jj)
                                                    if GB_comp!=[[]]:
                                                        s=s+1
                                                        writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits mismatch found in projectx compare to Guidebox DB'})
                                                    else:
                                                        t =t+1
                                        else:
                                            v=v+1
                                            writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits missing found in projectx compare to Guidebox DB'})
                        if i.get("cast") and i.get("directors"): #and i.get("writers") is None:#110
                            for j in i.get("cast"):
                                v1=unidecode.unidecode(j.get("name"))
                                v2=unidecode.unidecode(j.get("character_name"))
                                if v2=="Himself":
                                    v2=''
                                arr_cast.append(['Actor',v1.lower(),v2.lower()])
                            for k in i.get("directors"):
                                d1=unidecode.unidecode(k.get("name"))
                                arr_director.append(['Director',d1.lower(),''])
                                arr_gb_cast.extend(arr_cast)
                                arr_gb_cast.extend(arr_director)
                            query3="select projectx_id from projectx.ProjectxMaps where source_id=%s and type='Program' and data_source='GuideBox' and sub_type='SE';"
                            cur1.execute(query3,(str(g)))
                            res3=cur1.fetchall()
                            if list(res3)==[]:
                                r=r+1
                            else:
                                for j in res3:
                                    for k in j:
                                        query4="SELECT name,lower(fullName),lower(part_name) FROM projectx.ProgramCredit inner join projectx.Credits on ProgramCredit.credit_id=Credits.id inner join projectx.CreditTypes on CreditTypes.id=ProgramCredit.credit_type where program_id=%s;"
                                        cur1.execute(query4,(k,))
                                        res4=cur1.fetchall()
                                        if res4:
                                            for l in res4:
                                                arr_px_cast.append(map(str,[x for x in list(l)]))
                                            for w in arr_px_cast:
                                                w[1]=unicode(w[1], "utf-8")
                                                w[1]=unidecode.unidecode(w[1])
                                                w[2]=unicode(w[2], "utf-8")
                                                w[2]=unidecode.unidecode(w[2])
                                                arr_PX_cast.append(w)
                                            if arr_PX_cast and arr_gb_cast:
                                                GB=Counter(map(str,[y for y in arr_gb_cast]))
                                                Px=Counter(map(str,[y for y in arr_PX_cast]))
                                                GB_px=GB-Px
						if list(GB_px.elements()):
                                                    GB_comp.append(list(GB_px.elements()))
                                                if GB_comp==[]:
                                                    t =t+1
                                                if GB_comp:
                                                    for x in GB_comp:
                                                        for xx in range(0,len(x)):
                                                            for yy in arr_PX_cast:
                                                                if yy[0]==(eval(x[xx]))[0]=='Actor' or yy[0]==(eval(x[xx]))[0]=='Director':
                                                                    if (eval(x[xx]))[1] in yy:
                                                                        if x:
                                                                            x[xx]="['None']"
                                                    for jj in range(len(GB_comp[0])-1,-1,-1):
                                                        if (GB_comp[0])[jj]=="['None']":
                                                                (GB_comp[0]).pop(jj)
                                                    if GB_comp!=[[]]:
                                                        s =s+1
                                                        writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits mismatch found in projectx compare to Guidebox DB'})
                                                    else:
                                                        t =t+1
                                        else:
                                            v=v+1
                                            writer.writerow({"GB_id":str(g),"Projectx_id_GB":str(k),"show_type":i.get("show_type").encode(),"Missing Credits from Guidebox":GB_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits missing found in projectx compare to Guidebox DB'})

	    except (AttributeError, pymongo.errors.OperationFailure,pymysql.OperationalError) as e:
	        print ("exception caught ",e)
	        continue
	    print name
	    print ("total GB_id",Total)
	    print ("not ingested",r)
    	    print ("Credits fail",s)
	    print ("credit pass",t)
	    print("Both have no credits",u)
	    print("projectx credits missing", v)					
    connection.close()        
    print datetime.datetime.now()
t1=threading.Thread(target=GB_credits,args=(0,"starting thread-1",50001,1))
t1.start()
t2=threading.Thread(target=GB_credits,args=(50001,"starting thread-2",100001,2))
t2.start()


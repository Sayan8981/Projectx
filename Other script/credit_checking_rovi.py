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
import sys
sys.setrecursionlimit(2000)

class Credits:
    jj=0
    hh=0
    total=0
    arr=[]
    p=0
    q=0
    m=0
    l=0
    z=0
    h=0
    e=0
    def rovi_credits(self,jj,hh,e):
	print("Starting: ",jj)
	print("ending: ",hh)
	print("index",e)
        conn=pymysql.connect(user="branch",passwd="(branch)",host="192.168.86.7",db="branch_service_rovi")
        cur=conn.cursor()

        conn1=pymysql.connect(user="projectx",passwd="projectx",host="54.173.42.225",db="projectx",port=3370)
        cur1=conn1.cursor()

        result_sheet='/Validation_Credits_rovi%d.csv'%e
        if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
    	csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    	w=open(os.getcwd()+result_sheet,"wa")
    	with w as mycsvfile:
            fieldnames = ["Rovi_id","Projectx_id_rovi","Missing Credits from Rovi","Duplicates credits in projectx","Result of populated Credits","Comment"]
            writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
            writer.writeheader()
            query1="SELECT distinct(program_id) FROM branch_service_rovi.rovi_programs_credits where program_id between %s and %s;"
            cur.execute(query1,(jj,hh,))
            res1=cur.fetchall()
            for i in res1:
	        try:
	            for x in i:
                        print ("rovi_id: ",x)
                        arr_rovi=[]
                        arr_Rovi=[]
                        arr_px=[]
	                arr_PX=[]
                        rovi_comp=[]
	                px_comp=[]      
                        if x is not None:
			    Credits.total=Credits.total+1
                            query2="SELECT credit_type,lower(full_credit_name),lower(part_name),sequence_number FROM branch_service_rovi.rovi_programs_credits where program_id=%s and credit_id is not NULL;"
                            cur.execute(query2,(str(x),))
                            res2=cur.fetchall()
                            for i in res2:
                                arr_rovi.append(map(str,[k for k in list(i)]))
                            for v in arr_rovi:
                                if v[3]=='None':
                                    v[3]='0'
	                        v[1]=unicode(v[1], "utf-8")
		                v[1]=unidecode.unidecode(v[1])
		                v[2]=unicode(v[2], "utf-8")
                                v[2]=unidecode.unidecode(v[2])	
                                arr_Rovi.append(v)
                            query3="select projectx_id from projectx.ProjectxMaps where source_id=%s and type='Program' and data_source='Rovi';" 
                            cur1.execute(query3,(str(x),))
                            res3=cur1.fetchall()
		            arr=[]
                            if list(res3)==[]:
                                Credits.p=Credits.p+1
                                print("this Rovi_id not ingested in Px ")
                            else:
                                for j in res3:
                                    for k in j:
		                        query4="SELECT name,lower(fullName),lower(part_name),sequence FROM projectx.ProgramCredit inner join projectx.Credits on ProgramCredit.credit_id=Credits.id inner join projectx.CreditTypes on CreditTypes.id=ProgramCredit.credit_type where program_id=%s and data_source='Rovi';"
                                        cur1.execute(query4,(k,))
                                        res4=cur1.fetchall()
                                        for l in res4:
                                            arr_px.append(map(str,[i for i in list(l)]))
		                        for w in arr_px:
                                            w[1]=unicode(w[1], "utf-8")
                                            w[1]=unidecode.unidecode(w[1])
                                            w[2]=unicode(w[2], "utf-8")
                                            w[2]=unidecode.unidecode(w[2])
                                            arr_PX.append(w)
				        if arr_PX and arr_Rovi:
                                            Rovi=Counter(map(str,[i for i in arr_Rovi]))
                                            Px=Counter(map(str,[i for i in arr_PX]))
                                            rovi_px=Rovi-Px
                                            if list(rovi_px.elements()):
                                                rovi_comp.append(list(rovi_px.elements()))
				            for i in arr_PX:
                                                if arr_PX.count(i)>1:
                                                    if i not in px_comp:
                                                        px_comp.append(i)
                                            if rovi_comp==[] and px_comp==[]:
                                                Credits.q =Credits.q+1
                                                print("Pass from rovi :", Credits.q)
                                            if px_comp and rovi_comp:
				                Credits.m=Credits.m+1
                                                writer.writerow({"Rovi_id":str(x),"Projectx_id_rovi":str(k),"Missing Credits from Rovi":rovi_comp,"Duplicates credits in projectx":px_comp,"Result of populated Credits":'Fail',"Comment":'Credits mismatch and duplicate found in  projectx compare to Rovi DB'})
			  	            if px_comp and not rovi_comp:
				                Credits.m=Credits.m+1
                                                writer.writerow({"Rovi_id":str(x),"Projectx_id_rovi":str(k),"Missing Credits from Rovi":'',"Duplicates credits in projectx":px_comp,"Result of populated Credits":'Fail',"Comment":'Credits mismatch and duplicate found in  projectx compare to Rovi DB'})
				            if not px_comp and rovi_comp:
				                Credits.m=Credits.m+1
				                writer.writerow({"Rovi_id":str(x),"Projectx_id_rovi":str(k),"Missing Credits from Rovi":rovi_comp,"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits mismatch and duplicate found in  projectx compare to Rovi DB'})
                       		        if not arr_PX and arr_Rovi:
					    Credits.l=Credits.l+1
				            writer.writerow({"Rovi_id":str(x),"Projectx_id_rovi":str(k),"Missing Credits from Rovi":'',"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits missing in projectx DB'})
					if arr_PX and not arr_Rovi:
                                            Credits.h=Credits.h+1
                                            print({"Rovi_id":str(x),"Projectx_id_rovi":str(k),"Missing Credits from Rovi":'',"Duplicates credits in projectx":'',"Result of populated Credits":'Fail',"Comment":'Credits missing in rovi DB'})
				        if not arr_PX and not arr_Rovi:
					    Credits.z=Credits.z+1
				            print({"Rovi_id":str(x),"Projectx_id_rovi":str(k),"Missing Credits from Rovi":'',"Duplicates credits in projectx":'',"Result of populated Credits":'',"Comment":'Credits missing from both DB'})
			print("Total counts of Rovi id: ",Credits.total)
              	        print("Pass :",Credits.q )
			print("only rovi credits present",Credits.l)
			print("only projectx credits present",Credits.h)
            	        print("Not ingested id: ", Credits.p)
            	        print("Credit mismatch count :", Credits.m)
			print("both have no crdits",Credits.z) 
	        except pymysql.OperationalError:
                    print("caught exception")                                                     
                    continue        
                                    

    def func(self):
	Credits.e=Credits.e+1
	Credits.jj=1+Credits.hh	
	Credits.hh=Credits.jj+19999
	while Credits.hh<300000:
	    self.rovi_credits(Credits.jj,Credits.hh,Credits.e)
	    self.func()
a=Credits()
a.func()

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

class multiple_ids:

    conn1=pymysql.connect(user="projectx",passwd="projectx",host="34.201.242.195",db="projectx",port=3370)
    cur1=conn1.cursor()

    def id_search(self,source):
	result_sheet='/multiple_mapped_ids_for_same_source_id.csv'
        if(os.path.isfile(os.getcwd()+result_sheet)):
            os.remove(os.getcwd()+result_sheet)
        csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
        w=open(os.getcwd()+result_sheet,"wa")
        with w as mycsvfile:
            fieldnames = ["Service","projectx_ott_source_id","Link_Source_id","Projectx_ids which have same source_id","comment"]
            writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
            writer.writeheader()
	    array=[]
	    array1=[]
	    total=0
	    Gb=0
	    Rv=0
	    GB_RV=0
	    dict_ids=dict()
	    for jj in source: 
	        query2="SELECT distinct ProjectxMaps.projectx_id,ProgramOtts.source_id FROM ProjectxMaps join ProgramOtts on ProjectxMaps.projectx_id=ProgramOtts.program_id where ProjectxMaps.map_reason='new' and ProjectxMaps.type='Program' and ProjectxMaps.sub_type='MO' and ProjectxMaps.data_source='GuideBox' and ott_source=%s;"
	        self.cur1.execute(query2,(jj[0]),)
	        res2=self.cur1.fetchall()
	        for i in res2:
		    print jj
	            total=total+1	
	            query3="SELECT distinct ProgramOtts.program_id,ProgramOtts.data_source FROM projectx.ProgramOtts where is_deleted='0' and source_id=%s and ott_source=%s;" 	
		    self.cur1.execute(query3,(str(i[1]),jj[0]),)
	  	    res3=self.cur1.fetchall()
		    if list(res3)!=[]:
		        for aa in res3:
                            array1.append(list(aa))
		        if len(array1)>1:
		 	    GB_RV=GB_RV+1
		 	    key=str(jj[1])+':'+str(i[1])
			    dict_ids.setdefault(key,[])
		  	    dict_ids[key]=[array1]
			    writer.writerow({"Service":jj[1],"projectx_ott_source_id":str(jj[0]),"Link_Source_id":str(i[1]),"Projectx_ids which have same source_id":dict_ids,"comment":'multiple mapped ids for same service_id '})
		        if len(array1)==1:
		    	
		   	    if (array1[0])[1]=='GuideBox':
			        Gb=Gb+1
			        key=str(jj[1])+':'+str(i[1])
                                dict_ids.setdefault(key,[])
                                dict_ids[key]=[array1]
                                print({"Service":jj[1],"projectx_ott_source_id":str(jj[0]),"Link_Source_id":str(i[1]),"Projectx_ids which have same source_id":dict_ids,"comment":'Mapped only with GuideBox'})
		    	    if (array1[0])[1]=='Rovi':
			        Rv=Rv+1
                                key=str(jj[1])+':'+str(i[1])
                                dict_ids.setdefault(key,[])
                                dict_ids[key]=[array1]
                                print({"Service":jj[1],"projectx_ott_source_id":str(jj[0]),"Link_Source_id":str(i[1]),"Projectx_ids which have same source_id":dict_ids,"comment":'Mapped only with Rovi'})
		        array1=[]
                        dict_ids={}
	                print({"total":total,"multiple_ids":GB_RV,"only GB":Gb,"only Rovi":Rv})
		
	print datetime.datetime.now()


    def link_search(self):
	arr=[]
	query1="SELECT distinct OttSources.id,OttSources.name FROM projectx.OttSources inner join projectx.ProgramOtts on ProgramOtts.ott_source=OttSources.id where name in ('amazon','itunes','netflixusa','vudu','HBO','hulu','hbogo','tbs','tnt','cbs','starz','youtube') order by id asc;"
	self.cur1.execute(query1)
	res1=self.cur1.fetchall()
	print res1
        for i in res1:
	    arr.append(list(i))
        print (arr)
        self.id_search(arr)


a=multiple_ids()
a.link_search()

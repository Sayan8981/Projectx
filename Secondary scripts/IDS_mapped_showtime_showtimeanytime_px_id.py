
import MySQLdb
import collections
from pprint import pprint
import sys
import csv
import os
import pymysql
import pymongo
import datetime
import sys
import urllib2
import json
import os
from urllib2 import HTTPError
import httplib
import socket


def checking_ids():

    inputFile="Not_ingested_ids_showtime"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    conn1=pymysql.connect(user="root",passwd="branch@123",host="localhost",db="branch_service")
    cur1=conn1.cursor()

    result_sheet='/No_ingestion_test_showtime_showtimeanytime_all_ids.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["showtime_id MO","showtime_id SE","showtime_id SM","projectx_id_showtime","category","series_name","Movie_name","Release_year","Season_number","Episode_number","Episode_title","Links","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()

	for r in range(1,len(fullist)):
            showtime_id_mo=fullist[r][0]
            showtime_id_se=fullist[r][1]
            projectx_id_showtime=fullist[r][3]
            comment=fullist[r][4]
	    print showtime_id_mo
            if showtime_id_mo:
		query1="select title,url,year from showtime_anytime_programs where expired=0 and source_program_id=%s;"
		cur1.execute(query1,(showtime_id_mo))
		res1=cur1.fetchall()
		if list(res1)!=[]:
                    for i in res1:
                        writer.writerow({"showtime_id MO":showtime_id_mo,"projectx_id_showtime":projectx_id_showtime,"category":'Showtime',"Movie_name":i[0],"Release_year":i[2],"Links":i[1],"Comment":comment})
		else:
                    query1="select title,url,year from showtime_programs where expired=0 and source_program_id=%s;"
                    cur1.execute(query1,(showtime_id_mo))
                    res1=cur1.fetchall()
                    for i in res1:
                        writer.writerow({"showtime_id MO":showtime_id_mo,"projectx_id_showtime":projectx_id_showtime,"category":'Showtime',"Movie_name":i[0],"Release_year":i[2],"Links":i[1],"Comment":comment}) 
            if showtime_id_se:
                query1="select series_id,title,url,season_number,episode_number,year from showtime_programs where expired=0 and source_program_id=%s;"
                cur1.execute(query1,(showtime_id_se))
                res1=cur1.fetchall()
                if list(res1)!=[]:
                    query2="select title from showtime_programs where expired=0 and source_program_id=%s"
		    cur1.execute(query2,((res1[0])[0]))
		    res2=cur1.fetchall()
                    writer.writerow({"showtime_id SE":showtime_id_se,"projectx_id_showtime":projectx_id_showtime,"category":'Showtime',"series_name":(res2[0])[0],"Release_year":(res1[0])[5],"Season_number":(res1[0])[3],"Episode_number":(res1[0])[4],"Episode_title":(res1[0])[1],"Links":(res1[0])[2],"Comment":comment})
                else:
		    query1="select series_id,title,url,season_number,episode_number,year from showtime_anytime_programs where expired=0 and source_program_id=%s;"
                    cur1.execute(query1,(showtime_id_se))
		    import pdb;pdb.set_trace()
                    res1=cur1.fetchall()
                    query2="select title from showtime_anytime_programs where expired=0 and source_program_id=%s"
                    cur1.execute(query2,((res1[0])[0]))
                    res2=cur.fetchall()
                    writer.writerow({"showtime_id SE":showtime_id_se,"projectx_id_showtime":projectx_id_showtime,"category":'Showtime',"series_name":(res2[0])[0],"Release_year":(res1[0])[5],"Season_number":(res1[0])[3],"Episode_number":(res1[0])[4],"Episode_title":(res1[0])[1],"Links":(res1[0])[2],"Comment":comment})


		
checking_ids()

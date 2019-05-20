import threading
import httplib
import socket
import urllib2
import MySQLdb
import collections
from pprint import pprint
import sys
import csv
import os
import pymysql
import datetime
from urllib2 import HTTPError
from collections import Counter
import json
import pymongo

def part_number_checking():

    inputFile="Result_mapping_Episodes"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/part_number_contains.csv'
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["Gb_sm_id","Gb_id","Rovi_id","episode_title","ozoneepisodetitle","OzoneOriginalEpisodeTitle","projectx_id_rovi","projectx_id_gb","Episode mapping","Comment"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()

        for r in range(1,len(fullist)):
            
            Gb_sm_id=fullist[r][0]
            Gb_id=fullist[r][1]
            Rovi_id=fullist[r][2]
            episode_title=str(fullist[r][3])
            ozoneepisodetitle=fullist[r][4]
            OzoneOriginalEpisodeTitle=fullist[r][5]
            projectx_id_rovi=fullist[r][6]
            projectx_id_gb=fullist[r][7]
            Episode_mapping =str(fullist[r][8])
            Comment=str(fullist[r][8])
              
            if Comment=='Fail' and Episode_mapping=='Fail':

                if "Part " in episode_title:
                    print episode_title 
                    writer.writerow({"Gb_sm_id":Gb_sm_id,"Gb_id":Gb_id,"Rovi_id":Rovi_id,"episode_title":episode_title,"ozoneepisodetitle":ozoneepisodetitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"projectx_id_rovi":projectx_id_rovi,"projectx_id_gb":projectx_id_gb,"Episode mapping":Episode_mapping,"Comment":Comment})



part_number_checking()

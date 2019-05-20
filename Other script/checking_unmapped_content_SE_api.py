"""writer:Saayan"""

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import MySQLdb
import collections
from pprint import pprint
import sys
import urllib2
import json
import os
from urllib2 import HTTPError
from urllib2 import URLError
import csv
import urllib
import os
import pymysql
import datetime
import httplib
import socket
import unidecode
sys.setrecursionlimit(2000)
import threading
 
def open_csv(start,name,end,id):
    inputFile="unmapped_content_SE"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/GuideBoxValidationTVSHowPreProd_PX_Saayan%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["Id","Title","TotalEpisodes","ReleaseYear","Gb_id","Gb_id_PX","Season Number","Episode Number","EpisodeTitle","OzoneOriginalEpisodeTitle","OzoneEpisodeTitle","OzoneRoviId","Scheme","Search","Match","AmazonLink","Amazon_Flag","StarzLink","Starz_Flag","NetflixLink","Netflix_flag","NBCLink","NBC_flag","CBSLink","CBS_flag","VUDULink","VUDU_flag","ITUNESLink","ITUNES_flag","Ott_flag","Result","Ozone_Series_id","Px_series_id","Rovi_id","Px_series_title","Px_episode_title","Px_release_year","Px_season_number","Px_episode_number","projectx_id","amazon_flag","starz_flag","netflix_flag","cbs_flag","vudu_flag","itunes_flag","comment","series_match","episode_title_match","title_match","Season_number_match","Episode_number_match","Release_year_match"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        total=0
        for r in range(start,end-1):
            total=total+1
            print ({"thread_name":name,"total":total})
            source_amazon=[]
            source_starz=[]
            source_netflix=[]
            source_cbs=[]
            source_vudu=[]
            source_itunes=[]

            arr_px=[]
            arr_rovi=[]
	    arr_gb=[]
            sec_arr=[]
            s=0
            t=0
            u=0
            v=0
            w=0
            x=0

            Result=str(fullist[r][29])
            if Result=="MAP FAIL":
                Id=str(fullist[r][0])
                Title=unicode(str(fullist[r][1]),'utf-8')
                Title=unidecode.unidecode(Title)
                TotalEpisodes=str(fullist[r][2])
                ReleaseYear=str(fullist[r][3])
                Gb_id=str(fullist[r][4])
                Season_Number=str(fullist[r][5])
                Episode_Number=str(fullist[r][6])
                EpisodeTitle=unicode(str(fullist[r][7]),'utf-8')
                EpisodeTitle=unidecode.unidecode(EpisodeTitle)
                OzoneOriginalEpisodeTitle=str(fullist[r][8])
                OzoneEpisodeTitle=str(fullist[r][9])
                OzoneRoviId=str(fullist[r][10])
                Scheme=str(fullist[r][11])
                Search=str(fullist[r][12])
                Match=str(fullist[r][13])
                AmazonLink=str(fullist[r][14])
                Amazon_Flag=str(fullist[r][15])
                StarzLink=str(fullist[r][16])
                Starz_Flag=str(fullist[r][17])
                NetflixLink=str(fullist[r][18])
                Netflix_flag=str(fullist[r][19])
                NBCLink=str(fullist[r][20])
                NBC_flag=str(fullist[r][21])
                CBSLink=str(fullist[r][22])
                CBS_flag=str(fullist[r][23])
                VUDULink=str(fullist[r][24])
                VUDU_flag=str(fullist[r][25])
                ITUNESLink=str(fullist[r][26])
                ITUNES_flag=str(fullist[r][27])
                Ott_flag=str(fullist[r][28])
                Result=str(fullist[r][29])
                Ozone_Series_id=str(fullist[r][30])
                print Result
                print Gb_id 
                try:
                    try:
                        if eval(AmazonLink):
                            source_amazon=[]
                            for oo in eval(AmazonLink):
                                source_amazon.append(oo)
                            for l in source_amazon:
                                if source_amazon.count(l)>1:
                                    source_amazon.remove(l)
                    except SyntaxError:
                        source_amazon=[0]

                    try:
                        if eval(StarzLink):
                            source_starz=[]
                            for oo in eval(StarzLink):
                                source_starz.append(oo)
                            for l in source_starz:
                                if source_starz.count(l)>1:
                                    source_starz.remove(l)
                    except SyntaxError:
                        source_starz=[0]
                    try:
                        if eval(NetflixLink):
                            source_netflix=[]
                            for oo in eval(NetflixLink):
                                source_netflix.append(oo)
                            for l in source_netflix:
                                if source_netflix.count(l)>1:
                                    source_netflix.remove(l)
                    except SyntaxError:
                        source_netflix=[0]
                    try:
                        if eval(CBSLink):
                            source_cbs=[]
                            for oo in eval(CBSLink):
                                source_cbs.append(oo)
                            for l in source_cbs:
                                if source_cbs.count(l)>1:
                                    source_cbs.remove(l)
                    except SyntaxError:
                        source_cbs=[0]
                    try:
                        if eval(VUDULink):
                            source_vudu=[]
                            for oo in eval(VUDULink):
                                source_vudu.append(oo)
                            for l in source_vudu:
                                if source_vudu.count(l)>1:
                                    source_vudu.remove(l)
                    except SyntaxError:
                        source_vudu=[0]
                    try:
                        if eval(ITUNESLink):
                            source_itunes=[]
                            for oo in eval(ITUNESLink):
                                source_itunes.append(oo)
                            for l in source_itunes:
                                if source_itunes.count(l)>1:
                                    source_itunes.remove(l)
                    except SyntaxError:
                        source_itunes=[0]
                    #import pdb;pdb.set_trace()
                    if source_amazon!=[0]:    
                        url_amazon="http://34.231.212.186:81/projectx/%s/amazon/ottprojectx"%source_amazon[0]
                        response_amazon=urllib2.Request(url_amazon) 
                        response_amazon.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_amazon=urllib2.urlopen(response_amazon)
                        data_amazon=resp_amazon.read()
                        data_resp_amazon=json.loads(data_amazon)
                        for ii in data_resp_amazon:
                            if ii.get("sub_type")=="SE" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
                                arr_px.append(ii.get("projectx_id"))
                                arr_gb.append(ii.get("source_id"))
                            if ii.get("type")=='Program' and ii.get("data_source")=='Rovi':
                                arr_px.append(ii.get("projectx_id"))
                                arr_rovi.append(ii.get("source_id"))
                        for aa in arr_px:
                            if arr_px.count(aa)>1:
                                arr_px.remove(aa)
                        for jj in arr_px:
                            sec_arr.append(jj)
                        s=len(sec_arr)
                        if len(sec_arr)>=1:
                            amazon_flag='True'
                        else:
                            amazon_flag='False'
                    else:
                        amazon_flag=''
                    arr_px=[]
                    if source_starz!=[0]:
                        url_starz="http://34.231.212.186:81/projectx/%s/starz/ottprojectx"%source_starz[0]
                        response_starz=urllib2.Request(url_starz)
                        response_starz.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_starz=urllib2.urlopen(response_starz)
                        data_starz=resp_starz.read()
                        data_resp_starz=json.loads(data_starz)
                        for ii in data_resp_starz:
                            if ii.get("sub_type")=="SE" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
                                arr_px.append(ii.get("projectx_id"))
                                arr_gb.append(ii.get("source_id"))
                            if ii.get("type")=='Program' and ii.get("data_source")=='Rovi':
                                arr_px.append(ii.get("projectx_id"))
                                arr_rovi.append(ii.get("source_id"))
                        for aa in arr_px:
                            if arr_px.count(aa)>1:
                                arr_px.remove(aa)
                        for jj in arr_px:
                            sec_arr.append(jj)
                       
                        t=len(sec_arr)
                        if len(sec_arr)>s:
                            starz_flag='True'
                        else:
                            starz_flag='False'    
                    else:
                        starz_flag=''
                    arr_px=[]
                    if source_netflix!=[0]: 
                        url_netflix="http://34.231.212.186:81/projectx/%s/netflixusa/ottprojectx"%source_netflix[0]
                        response_netflix=urllib2.Request(url_netflix)
                        response_netflix.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_netflix=urllib2.urlopen(response_netflix)
                        data_netflix=resp_netflix.read()
                        data_resp_netflix=json.loads(data_netflix)
                        for ii in data_resp_netflix:
                            if ii.get("sub_type")=="SE" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
                                arr_px.append(ii.get("projectx_id"))
                                arr_gb.append(ii.get("source_id"))
                            if ii.get("type")=='Program' and ii.get("data_source")=='Rovi':
                                arr_px.append(ii.get("projectx_id"))
                                arr_rovi.append(ii.get("source_id"))
                        for aa in arr_px:
                            if arr_px.count(aa)>1:
                                arr_px.remove(aa)
                        for jj in arr_px:
                            sec_arr.append(jj)
                        
                        u=len(sec_arr)
                        if len(sec_arr)>t:         
                            netflix_flag='True'
                        else:
                            netflix_flag='False'
                    else:
                        netflix_flag=''
                    arr_px=[]
                    if source_cbs!=[0]:   
                        url_cbs="http://34.231.212.186:81/projectx/%s/cbs/ottprojectx"%source_cbs[0]
                        response_cbs=urllib2.Request(url_cbs)
                        response_cbs.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_cbs=urllib2.urlopen(response_cbs)
                        data_cbs=resp_cbs.read()
                        data_resp_cbs=json.loads(data_cbs)
                        for ii in data_resp_cbs:
                            if ii.get("sub_type")=="SE" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
                                arr_px.append(ii.get("projectx_id"))
                                arr_gb.append(ii.get("source_id"))
                            if ii.get("type")=='Program' and ii.get("data_source")=='Rovi':
                                arr_px.append(ii.get("projectx_id"))
                                arr_rovi.append(ii.get("source_id"))
                        for aa in arr_px:
                            if arr_px.count(aa)>1:
                                 arr_px.remove(aa)
                        for jj in arr_px:
                            sec_arr.append(jj)
                        
                        v=len(sec_arr)
                        if len(sec_arr)>u:
                            cbs_flag='True'
                        else:
                            cbs_flag='False'
                    else:
                        cbs_flag=''
		    arr_px=[]
                    if source_vudu!=[0]:
                        url_vudu="http://34.231.212.186:81/projectx/%s/vudu/ottprojectx"%source_vudu[0]
                        response_vudu=urllib2.Request(url_vudu)
                        response_vudu.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_vudu=urllib2.urlopen(response_vudu)
                        data_vudu=resp_vudu.read()
                        data_resp_vudu=json.loads(data_vudu)
                        for ii in data_resp_vudu:
                            if ii.get("sub_type")=="SE" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
                                arr_px.append(ii.get("projectx_id"))
                                arr_gb.append(ii.get("source_id"))
                            if ii.get("type")=='Program' and ii.get("data_source")=='Rovi':
                                arr_px.append(ii.get("projectx_id"))
                                arr_rovi.append(ii.get("source_id"))
                        for aa in arr_px:
                            if arr_px.count(aa)>1:
                                arr_px.remove(aa)
                        for jj in arr_px:
                            sec_arr.append(jj)
                          
                        w=len(sec_arr)
                        if len(sec_arr)>v:  
                            vudu_flag='True'
                        else:
                            vudu_flag='False'
                    else:
                        vudu_flag=''
                    arr_px=[]
                    if source_itunes!=[0]:
                        url_itune="http://34.231.212.186:81/projectx/%s/itunes/ottprojectx"%source_itunes[0]
                        response_itune=urllib2.Request(url_itune)
                        response_itune.add_header('Authorization','Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3')
                        resp_itune=urllib2.urlopen(response_itune)
                        data_itune=resp_itune.read()
                        data_resp_itune=json.loads(data_itune)
                        for ii in data_resp_itune:
                            if ii.get("sub_type")=="SE" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
                                arr_px.append(ii.get("projectx_id"))
                                arr_gb.append(ii.get("source_id"))
                            if ii.get("type")=='Program' and ii.get("data_source")=='Rovi':
                                arr_px.append(ii.get("projectx_id"))
                                arr_rovi.append(ii.get("source_id"))
                        for aa in arr_px:
                            if arr_px.count(aa)>1:
                                arr_px.remove(aa)
                        for jj in arr_px:
                            sec_arr.append(jj)
                        
                        x=len(sec_arr)
                        if len(sec_arr)>w: 
                            itunes_flag='True'
                        else:
                            itunes_flag='False'
                    else:
                        itunes_flag=''
 
                    for bb in sec_arr:
                        if sec_arr.count(bb)>1:
                            sec_arr.remove(bb)
                        if bb in sec_arr:
                            if sec_arr.count(bb)>1:
                                sec_arr.remove(bb)

                    for bb in arr_rovi:
                        if arr_rovi.count(bb)>1:
                            arr_rovi.remove(bb)
                        if bb in arr_rovi:
                            if arr_rovi.count(bb)>1:
                                arr_rovi.remove(bb)
                    for bb in arr_gb:
                        if arr_gb.count(bb)>1:
                            arr_gb.remove(bb)
                        if bb in arr_gb:
                            if arr_gb.count(bb)>1:
                                arr_gb.remove(bb)
                    if amazon_flag=='True' or starz_flag=='True' or netflix_flag=='True' or cbs_flag=='True' or vudu_flag=='True' or itunes_flag=='True':
                        if len(sec_arr)==1:
                            url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%sec_arr[0]
                            response_px=urllib2.Request(url_px)
                            response_px.add_header('Authorization','Token token=709d07cc85ab679bdc9c9a7124cead72f6465301c142bfdd2b7e5c2852c75b89')
                            resp_px=urllib2.urlopen(response_px)
                            data_px=resp_px.read()
                            data_resp_px=json.loads(data_px)
                            for kk in data_resp_px:
                                if kk.get("original_title")!='':
                                    series_title=unicode(kk.get("original_title"))
                                    series_title=unidecode.unidecode(series_title)
                                else:
                                    series_title=unicode(kk.get("long_title"))
                                    series_title=unidecode.unidecode(series_title)
                                if kk.get("original_episode_title")!='': 
                                    episode_title=unicode(kk.get("original_episode_title"))
                                    episode_title=unidecode.unidecode(episode_title)
                                    ratio_title=fuzz.ratio(episode_title.upper(),EpisodeTitle.upper())
                                    if ratio_title >=70:
                                         episode_title_match="Above"+'90%'
                                         title_match='Pass' 
                                    else:
                                         episode_title =unicode(kk.get("episode_title"))
                                         episode_title=unidecode.unidecode(episode_title)  
                                         ratio_title=fuzz.ratio(episode_title.upper(),EpisodeTitle.upper())
                                         if ratio_title >=70:
                                             episode_title_match="Above"+'90%'
                                             title_match='Pass'
                                         else:
                                             episode_title_match="Below"+'90%'
                                             title_match='Fail' 
                                else:
                                    episode_title =unicode(kk.get("episode_title"))
                                    episode_title=unidecode.unidecode(episode_title)
                                release_year=kk.get("release_year")
                                season_number=kk.get("episode_season_number")
                                episode_number=kk.get("episode_season_sequence")
                                series_id=str(kk.get("series_id"))
                                if Ozone_Series_id==series_id:
                                    series_match='Pass'
                                else:
                                    series_match='Fail/Not ingested' 
                                ratio_title=fuzz.ratio(episode_title.upper(),EpisodeTitle.upper())
                                if ratio_title >=70:
                                    episode_title_match="Above"+'90%'
                                    title_match='Pass'
                                else:
                                    episode_title_match="Below"+'90%'
                                    title_match='Fail'
                                if str(season_number)==Season_Number:
                                    Season_number_match="Pass"
                                else:
                                    Season_number_match='Fail'
                                if str(episode_number)==Episode_Number:
                                    Episode_number_match="Pass"
                                else:
                                    Episode_number_match='Fail'   
                                if str(release_year)==ReleaseYear:
                                    Release_year_match="Pass"
                                else:
                                    Release_year_match='Fail'
                            writer.writerow({"Id":Id,"Title":Title,"TotalEpisodes":TotalEpisodes,"ReleaseYear":ReleaseYear,"Gb_id":Gb_id,"Gb_id_PX":arr_gb,"Season Number":Season_Number,"Episode Number":Episode_Number,"EpisodeTitle":EpisodeTitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"OzoneEpisodeTitle":OzoneEpisodeTitle,"OzoneRoviId":OzoneRoviId,"Scheme":Scheme,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Ozone_Series_id":Ozone_Series_id,"EpisodeTitle":EpisodeTitle,"Px_series_id":series_id,"Px_series_title":series_title,"Px_episode_title":episode_title,"Px_release_year":release_year,"Px_season_number":season_number,"Px_episode_number":episode_number,"Rovi_id":arr_rovi,"projectx_id":sec_arr,"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'All link or any of them is present in projectx API',"series_match":series_match,"episode_title_match":episode_title_match,"title_match":title_match,"Season_number_match":Season_number_match,"Episode_number_match":Episode_number_match,"Release_year_match":Release_year_match})

                        if len(sec_arr)>1:
                            writer.writerow({"Id":Id,"Title":Title,"TotalEpisodes":TotalEpisodes,"ReleaseYear":ReleaseYear,"Gb_id":Gb_id,"Gb_id_PX":arr_gb,"Season Number":Season_Number,"Episode Number":Episode_Number,"EpisodeTitle":EpisodeTitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"OzoneEpisodeTitle":OzoneEpisodeTitle,"OzoneRoviId":OzoneRoviId,"Scheme":Scheme,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Ozone_Series_id":Ozone_Series_id,"Px_series_id":'',"Px_series_title":'',"Px_episode_title":'',"Px_release_year":'',"Px_season_number":'',"Px_episode_number":'',"Rovi_id":arr_rovi,"projectx_id":sec_arr,"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'Multiple projectx ids found',"series_match":'',"episode_title_match":'',"title_match":'',"Season_number_match":'',"Episode_number_match":'',"Release_year_match":''})
                    elif amazon_flag=='' and starz_flag=='' and netflix_flag=='' and cbs_flag=='' and vudu_flag=='' and itunes_flag=='':
                        writer.writerow({"Id":Id,"Title":Title,"TotalEpisodes":TotalEpisodes,"ReleaseYear":ReleaseYear,"Gb_id":Gb_id,"Gb_id_PX":arr_gb,"Season Number":Season_Number,"Episode Number":Episode_Number,"EpisodeTitle":EpisodeTitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"OzoneEpisodeTitle":OzoneEpisodeTitle,"OzoneRoviId":OzoneRoviId,"Scheme":Scheme,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Ozone_Series_id":Ozone_Series_id,"Px_series_id":'',"Px_series_title":'',"Px_episode_title":'',"Px_release_year":'',"Px_season_number":'',"Px_episode_number":'',"Rovi_id":'',"projectx_id":'',"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'this links not in the sheet',"series_match":'',"episode_title_match":'',"title_match":'',"Season_number_match":'',"Episode_number_match":'',"Release_year_match":''}) 
                    else:
                        writer.writerow({"Id":Id,"Title":Title,"TotalEpisodes":TotalEpisodes,"ReleaseYear":ReleaseYear,"Gb_id":Gb_id,"Gb_id_PX":arr_gb,"Season Number":Season_Number,"Episode Number":Episode_Number,"EpisodeTitle":EpisodeTitle,"OzoneOriginalEpisodeTitle":OzoneOriginalEpisodeTitle,"OzoneEpisodeTitle":OzoneEpisodeTitle,"OzoneRoviId":OzoneRoviId,"Scheme":Scheme,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Ozone_Series_id":Ozone_Series_id,"Px_series_id":'',"Px_series_title":'',"Px_episode_title":'',"Px_release_year":'',"Px_season_number":'',"Px_episode_number":'',"Rovi_id":'',"projectx_id":'',"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'this links not ingested',"series_match":'',"episode_title_match":'',"title_match":'',"Season_number_match":'',"Episode_number_match":'',"Release_year_match":''})
                except httplib.BadStatusLine:
                    print ("exception caught httplib.BadStatusLine...........................................")
                    continue
                except urllib2.HTTPError:
                    print ("exception caught HTTPError...........................................")
                    continue
                except socket.error:
                    print ("exception caught SocketError...........................................")
                    continue
                except URLError:
                    print ("exception caught URLError...........................................")
                    continue

    print datetime.datetime.now()
#open_csv()
t1 =threading.Thread(target=open_csv,args=(1,"thread - 1",19001,1))
t1.start()
t2 =threading.Thread(target=open_csv,args=(19001,"thread - 2",38311,2))
t2.start()

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
 
def checking_unmapped(start,name,end,id):
    inputFile="unmapped_content_MO"
    f = open(os.getcwd()+'/'+inputFile+'.csv', 'rb')
    reader = csv.reader(f)
    fullist=list(reader)

    result_sheet='/GuideBoxValidationMoviePreProd_PX_Saayan%d.csv'%id
    if(os.path.isfile(os.getcwd()+result_sheet)):
        os.remove(os.getcwd()+result_sheet)
    csv.register_dialect('excel',lineterminator = '\n',skipinitialspace=True,escapechar='')
    w=open(os.getcwd()+result_sheet,"wa")

    with w as mycsvfile:
        fieldnames = ["Gb_id","Title","ReleaseYear","Gb_id_PX","Search","Match","AmazonLink","Amazon_Flag","StarzLink","Starz_Flag","NetflixLink","Netflix_flag","NBCLink","NBC_flag","CBSLink","CBS_flag","VUDULink","VUDU_flag","ITUNESLink","ITUNES_flag","Ott_flag","Result","Rovi_id","Px_title","Px_release_year","projectx_id","amazon_flag","starz_flag","netflix_flag","cbs_flag","vudu_flag","itunes_flag","amazon_flag_expired","vudu_flag_expired","starz_flag_expired","netflix_flag_expired","cbs_flag_expired","itunes_flag_expired","comment","Duplicate id","movie_title_match","title_match","Release_year_match","Guidebox_id_match"]
        writer = csv.DictWriter(mycsvfile,fieldnames=fieldnames,dialect="excel",lineterminator = '\n')
        writer.writeheader()
        total=0
        Token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
        Token1='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
        domain_name="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com"
        for r in range(start,end-1):
            total=total+1
            print ({"thread_name":name,"total":total})
            source_amazon=[]
            source_starz=[]
            source_netflix=[]
            source_cbs=[]
            source_vudu=[]
            source_itunes=[]
            search_px_id=[]
            search_px_id_=[]
            search_px_id_filtered=[]

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

            Result=str(fullist[r][21])
            if Result=="Couldn't Map":
 #               import pdb;pdb.set_trace()
                Gb_id=str(fullist[r][0])
                Title=unicode(str(fullist[r][1]),'utf-8')
                Title=unidecode.unidecode(Title)
                ReleaseYear=str(fullist[r][2])
                Search=str(fullist[r][3])
                Match=str(fullist[r][4])
                AmazonLink=str(fullist[r][5])
                Amazon_Flag=str(fullist[r][6])
                StarzLink=str(fullist[r][7])
                Starz_Flag=str(fullist[r][8])
                NetflixLink=str(fullist[r][9])
                Netflix_flag=str(fullist[r][10])
                NBCLink=str(fullist[r][11])
                NBC_flag=str(fullist[r][12])
                CBSLink=str(fullist[r][13])
                CBS_flag=str(fullist[r][14])
                VUDULink=str(fullist[r][15])
                VUDU_flag=str(fullist[r][16])
                ITUNESLink=str(fullist[r][17])
                ITUNES_flag=str(fullist[r][18])
                Ott_flag=str(fullist[r][19])
                Result=str(fullist[r][21])
                print Result
                print Gb_id
                amazon_flag_expired=''
                vudu_flag_expired=''
                starz_flag_expired=''
                netflix_flag_expired=''
                cbs_flag_expired=''
                itunes_flag_expired='' 
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
                        response_amazon.add_header('Authorization',Token)
                        resp_amazon=urllib2.urlopen(response_amazon)
                        data_amazon=resp_amazon.read()
                        data_resp_amazon=json.loads(data_amazon)
                        for ii in data_resp_amazon:
                            if ii.get("sub_type")=="MO" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
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
                            expired_link="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=amazon"%source_amazon[0]
                            response_expired=urllib2.Request(expired_link)
                            response_expired.add_header('Authorization',Token1)
                            resp_exp=urllib2.urlopen(response_expired)
                            data_available=resp_exp.read()
                            data_resp_exp=json.loads(data_available)
                            if data_resp_exp.get("is_available")==False:
                                amazon_flag_expired='False'
                                amazon_flag='False'
                            else:
                                amazon_flag_expired='True'
                                amazon_flag='False'  
                            
                    else:
                        amazon_flag=''
                    arr_px=[]
                    if source_starz!=[0]:
                        url_starz="http://34.231.212.186:81/projectx/%s/starz/ottprojectx"%source_starz[0]
                        response_starz=urllib2.Request(url_starz)
                        response_starz.add_header('Authorization',Token)
                        resp_starz=urllib2.urlopen(response_starz)
                        data_starz=resp_starz.read()
                        data_resp_starz=json.loads(data_starz)
                        for ii in data_resp_starz:
                            if ii.get("sub_type")=="MO" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
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
                            expired_link="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=starz"%source_starz[0]
                            response_expired=urllib2.Request(expired_link)
                            response_expired.add_header('Authorization',Token1)
                            resp_exp=urllib2.urlopen(response_expired)
                            data_available=resp_exp.read()
                            data_resp_exp=json.loads(data_available)
                            if data_resp_exp.get("is_available")==False:
                                starz_flag_expired='False'
                                starz_flag='False'
                            else:
                                starz_flag_expired='True'
                                starz_flag='False' 
                         
                    else:
                        starz_flag=''
                    arr_px=[]
                    if source_netflix!=[0]: 
                        url_netflix="http://34.231.212.186:81/projectx/%s/netflixusa/ottprojectx"%source_netflix[0]
                        response_netflix=urllib2.Request(url_netflix)
                        response_netflix.add_header('Authorization',Token)
                        resp_netflix=urllib2.urlopen(response_netflix)
                        data_netflix=resp_netflix.read()
                        data_resp_netflix=json.loads(data_netflix)
                        for ii in data_resp_netflix:
                            if ii.get("sub_type")=="MO" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
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
                            expired_link="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=netflixusa"%source_netflix[0]
                            response_expired=urllib2.Request(expired_link)
                            response_expired.add_header('Authorization',Token1)
                            resp_exp=urllib2.urlopen(response_expired)
                            data_available=resp_exp.read()
                            data_resp_exp=json.loads(data_available)
                            if data_resp_exp.get("is_available")==False:
                                netflix_flag_expired='False'
                                netflix_flag='False'
                            else:
                                netflix_flag_expired='True'
                                netflix_flag='False'
                       
                    else:
                        netflix_flag=''
                    arr_px=[]
                    if source_cbs!=[0]:   
                        url_cbs="http://34.231.212.186:81/projectx/%s/cbs/ottprojectx"%source_cbs[0]
                        response_cbs=urllib2.Request(url_cbs)
                        response_cbs.add_header('Authorization',Token)
                        resp_cbs=urllib2.urlopen(response_cbs)
                        data_cbs=resp_cbs.read()
                        data_resp_cbs=json.loads(data_cbs)
                        for ii in data_resp_cbs:
                            if ii.get("sub_type")=="MO" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
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
                            expired_link="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=cbs"%source_cbs[0]
                            response_expired=urllib2.Request(expired_link)
                            response_expired.add_header('Authorization',Token1)
                            resp_exp=urllib2.urlopen(response_expired)
                            data_available=resp_exp.read()
                            data_resp_exp=json.loads(data_available)
                            if data_resp_exp.get("is_available")==False:
                                cbs_flag_expired='False'
                                cbs_flag='False'
                            else:
                                cbs_flag_expired='True'
                                cbs_flag='False'
                         
                    else:
                        cbs_flag=''
		    arr_px=[]
                    if source_vudu!=[0]:
                        url_vudu="http://34.231.212.186:81/projectx/%s/vudu/ottprojectx"%source_vudu[0]
                        response_vudu=urllib2.Request(url_vudu)
                        response_vudu.add_header('Authorization',Token)
                        resp_vudu=urllib2.urlopen(response_vudu)
                        data_vudu=resp_vudu.read()
                        data_resp_vudu=json.loads(data_vudu)
                        for ii in data_resp_vudu:
                            if ii.get("sub_type")=="MO" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
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
                            expired_link="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=netflixusa"%source_vudu[0]
                            response_expired=urllib2.Request(expired_link)
                            response_expired.add_header('Authorization',Token1)
                            resp_exp=urllib2.urlopen(response_expired)
                            data_available=resp_exp.read()
                            data_resp_exp=json.loads(data_available)
                            if data_resp_exp.get("is_available")==False:
                                vudu_flag_expired='False'
                                vudu_flag='False'
                            else:
                                vudu_flag_expired='True'
                                vudu_flag='False'
                          
                    else:
                        vudu_flag=''
                    arr_px=[]
                    if source_itunes!=[0]:
                        url_itune="http://34.231.212.186:81/projectx/%s/itunes/ottprojectx"%source_itunes[0]
                        response_itune=urllib2.Request(url_itune)
                        response_itune.add_header('Authorization',Token)
                        resp_itune=urllib2.urlopen(response_itune)
                        data_itune=resp_itune.read()
                        data_resp_itune=json.loads(data_itune)
                        for ii in data_resp_itune:
                            if ii.get("sub_type")=="MO" and ii.get("type")=='Program' and ii.get("data_source")=='GuideBox':
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
                            expired_link="https://preprod.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=netflixusa"%source_itunes[0]
                            response_expired=urllib2.Request(expired_link)
                            response_expired.add_header('Authorization',Token1)
                            resp_exp=urllib2.urlopen(response_expired)
                            data_available=resp_exp.read()
                            data_resp_exp=json.loads(data_available)
                            if data_resp_exp.get("is_available")==False:
                                itunes_flag_expired='False'
                                itunes_flag='False'
                            else:
                                itunes_flag_expired='True'
                                itunes_flag='False'
                         
                    else:
                        itunes_flag=''
                    for bb in sec_arr:
                        while sec_arr.count(bb)>1:
                            sec_arr.remove(bb)
                        while sec_arr.count(bb)>1:
                            sec_arr.remove(bb)

                    for bb in sec_arr:
                        while sec_arr.count(bb)>1:
                            sec_arr.remove(bb)
                    for bb in arr_rovi:
                        while arr_rovi.count(bb)>1:
                            arr_rovi.remove(bb)
                    for bb in arr_gb:
                        while arr_gb.count(bb)>1:
                            arr_gb.remove(bb)
                    print("Guidebox_id:",  arr_gb)

                    if amazon_flag=='True' or starz_flag=='True' or netflix_flag=='True' or cbs_flag=='True' or vudu_flag=='True' or itunes_flag=='True':
                        if len(sec_arr)==1:
                            url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%sec_arr[0]
                            response_px=urllib2.Request(url_px)
                            response_px.add_header('Authorization',Token)
                            resp_px=urllib2.urlopen(response_px)
                            data_px=resp_px.read()
                            data_resp_px=json.loads(data_px)
                            for kk in data_resp_px:
                                if kk.get("original_title")!='': 
                                    movie_title=unicode(kk.get("original_title"))
                                    movie_title=unidecode.unidecode(movie_title)
                                    ratio_title=fuzz.ratio(movie_title.upper(),Title.upper())
                                    if ratio_title >=70:
                                        movie_title_match="Above"+'90%'
                                        title_match='Pass' 
                                    else:
                                        movie_title =unicode(kk.get("long_title"))
                                        movie_title=unidecode.unidecode(movie_title)
                                        ratio_title=fuzz.ratio(movie_title.upper(),Title.upper())
                                        if ratio_title >=70:
                                            movie_title_match="Above"+'90%'
                                            title_match='Pass'
                                        else:
                                            movie_title_match="Below"+'90%'
                                            title_match='Fail'
                                else:
                                    movie_title =unicode(kk.get("long_title"))
                                    movie_title=unidecode.unidecode(movie_title)
                                release_year=kk.get("release_year")
                                ratio_title=fuzz.ratio(movie_title.upper(),Title.upper())
                                if ratio_title >=70:
                                    movie_title_match="Above"+'90%'
                                    title_match='Pass'
                                else:
                                    movie_title_match="Below"+'90%'
                                    title_match='Fail'
                                if str(release_year)==ReleaseYear:
                                    Release_year_match="Pass"
                                else:
                                    r_y=release_year
                                    r_ys=release_year
                                    r_y=r_y+1
                                    if str(r_y)==ReleaseYear:
                                        Release_year_match='Pass'
                                    else:
                                        r_ys=r_ys-1
                                        if str(r_ys)==ReleaseYear:
                                            Release_year_match='Pass'
                                        else: 
                                            Release_year_match='Fail'
                                if arr_gb:
                                    if str(arr_gb[0])==Gb_id:
                                        Guidebox_id_match="True"
                                    else:
                                        Guidebox_id_match="False"
                                else:
                                    Guidebox_id_match="" 
                            writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":movie_title,"Px_release_year":release_year,"Rovi_id":arr_rovi,"projectx_id":sec_arr,"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'All link or any of them is present in projectx API',"movie_title_match":movie_title_match,"title_match":title_match,"Release_year_match":Release_year_match,"Guidebox_id_match":Guidebox_id_match})

                        if len(sec_arr)>1:
                            arr_gb=[]
                            arr_rovi=[]
                            search_px_id__=[]
                            search_px_id1_=[]
                            search_px_id1=[]
                            next_page_url=""
                            duplicate="" 
                            data_resp_search=dict()
#                            import pdb;pdb.set_trace() 
                            search_api="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/v3/voice_search?q=%s&safe_search=false&credit_summary=true&credit_types=Actor&aliases=true&ott=true"%urllib2.quote(Title)
                            response_search=urllib2.Request(search_api)
                            response_search.add_header('User-Agent','Branch Fyra v1.0')
                            response_search.add_header('Authorization',Token) 
                            resp_search=urllib2.urlopen(response_search)
                            data_search=resp_search.read()
                            data_resp_search=json.loads(data_search)
                            if data_resp_search.get("top_results"):
                                for ii in data_resp_search.get("top_results"):
                                    if ii.get("action_type")=="ott_search" and ii.get("action_type")!="web_results" and ii.get("results"):
                                        for jj in ii.get("results"):
                                            if jj.get("object").get("show_type")=='MO' or jj.get("object").get("show_type")=='OT':
                                                search_px_id.append(jj.get("object").get("id"))
                                                search_px_id1=search_px_id1+search_px_id

                                if search_px_id:
                                    for mm in search_px_id:
                                        if mm in sec_arr:
                                            search_px_id_.append(mm)
                                        else:
                                            search_px_id_filtered.append(mm)


 
                                if len(search_px_id_)==1 or search_px_id_==[]:
                                    try:
                                        search_px_id1_.append(search_px_id_[0])
                                        search_px_id_=[]
                                        search_px_id=[]
                                        duplicate='False'
                                    except IndexError:
                                        search_px_id_=[]
                                        search_px_id=[]
                                        duplicate='False'
                                else:
                                    if search_px_id_!=search_px_id__:
                                        search_px_id__=search_px_id__+search_px_id_
                                        duplicate='True'
                                        search_px_id=[]
                                    else:
                                        search_px_id__=search_px_id__
                                        duplicate='True'
                                        search_px_id=[]
                                if duplicate=='False': 
                                    while data_resp_search.get("results"):
                                        for nn in data_resp_search.get("results"):
                                            if nn.get("action_type")=="ott_search" and (nn.get("results")==[] or nn.get("results")):
                                                next_page_url=nn.get("next_page_url")
                                                if next_page_url is not None: 
                                                    search_api1=domain_name+next_page_url.replace(' ',"%20")
                                                    if search_api1!=domain_name :
                                                        search_api=search_api1
                                                        response_search=urllib2.Request(search_api)
                                                        response_search.add_header('User-Agent','Branch Fyra v1.0')
                                                        response_search.add_header('Authorization',Token)
                                                        resp_search=urllib2.urlopen(response_search)
                                                        data_search=resp_search.read()
                                                        data_resp_search=json.loads(data_search)
                                                else:
                                                    data_resp_search={"resilts":[]}         
                                            else:
                                                data_resp_search={"resilts":[]}

                                            if data_resp_search.get("results"):
                                                for nn in data_resp_search.get('results'):
                                                    if nn.get("results"):
                                                        for jj in nn.get("results"):
                                                            if jj.get("object").get("show_type")=='MO' or jj.get("object").get("show_type")=='OT':
                                                                search_px_id.append(jj.get("object").get("id"))
                                                                search_px_id1=search_px_id1+search_px_id  
   
                                                    if search_px_id:
                                                        for mm in search_px_id:
                                                            if mm in sec_arr:
                                                                search_px_id_.append(mm)
                                                            else:
                                                                search_px_id_filtered.append(mm)
                                            
                                                    for ss in search_px_id_:
                                                        while search_px_id_.count(ss)>1:
                                                            search_px_id_.remove(ss)
       
                                                    for ss in search_px_id1_:
                                                        while search_px_id1_.count(ss)>1:
                                                            search_px_id1_.remove(ss)
                                                        while search_px_id1_.count(ss)>1:
                                                            search_px_id1_.remove(ss) 
  
                                                    if len(search_px_id_)==1 or search_px_id_==[]:
                                                        try:
                                                            search_px_id1_.append(search_px_id_[0]) 
                                                            search_px_id_=[] 
                                                            search_px_id=[] 
                                                            duplicate='False' 
                                                        except IndexError:
                                                            search_px_id_=[]
                                                            search_px_id=[]
                                                            duplicate='False'      
                                                    else:
                                                        if search_px_id_!=search_px_id__:
                                                            search_px_id__=search_px_id__+search_px_id_
                                                            duplicate='True'
                                                            search_px_id=[]
                                                        else:
                                                            search_px_id__=search_px_id__
                                                            duplicate='True'
                                                            search_px_id=[]
  
                            if search_px_id1:

                                if len(search_px_id__)>1 and duplicate=='True': 
                                    writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":'',"Px_release_year":'',"Rovi_id":arr_rovi,"projectx_id":sec_arr,"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'Multiple projectx ids found in search api',"Duplicate id":search_px_id__,"movie_title_match":'',"title_match":'',"Release_year_match":'',"Guidebox_id_match":''})
                                else:
                                    if search_px_id1_:
                                        url_px="http://preprod-projectx-1556298832.us-east-1.elb.amazonaws.com/programs/%d?&ott=true"%search_px_id1_[0]
                                        response_px=urllib2.Request(url_px)
                                        response_px.add_header('Authorization',Token)
                                        resp_px=urllib2.urlopen(response_px)
                                        data_px=resp_px.read()
                                        data_resp_px=json.loads(data_px)
                                        for kk in data_resp_px:
                                            if kk.get("original_title")!='':
                                                movie_title=unicode(kk.get("original_title"))
                                                movie_title=unidecode.unidecode(movie_title)
                                                ratio_title=fuzz.ratio(movie_title.upper(),Title.upper())
                                                if ratio_title >=70:
                                                    movie_title_match="Above"+'90%'
                                                    title_match='Pass'
                                                else:
                                                    movie_title =unicode(kk.get("long_title"))
                                                    movie_title=unidecode.unidecode(movie_title)
                                                    ratio_title=fuzz.ratio(movie_title.upper(),Title.upper())
                                                    if ratio_title >=70:
                                                        movie_title_match="Above"+'90%'
                                                        title_match='Pass'
                                                    else:
                                                        movie_title_match="Below"+'90%'
                                                        title_match='Fail'
                                            else:
                                                movie_title =unicode(kk.get("long_title"))
                                                movie_title=unidecode.unidecode(movie_title)
                                                release_year=kk.get("release_year")
                                                ratio_title=fuzz.ratio(movie_title.upper(),Title.upper())
                                            if ratio_title >=70:
                                                movie_title_match="Above"+'90%'
                                                title_match='Pass'
                                            else:
                                                movie_title_match="Below"+'90%'
                                                title_match='Fail'
                                            if str(release_year)==ReleaseYear:
                                                Release_year_match="Pass"
                                            else :
                                                r_y=release_year
                                                r_ys=release_year
                                                r_y=r_y+1
                                                if str(r_y)==ReleaseYear:
                                                    Release_year_match='Pass'
                                                else:
                                                    r_ys=r_ys-1
                                                    if str(r_ys)==ReleaseYear:
                                                        Release_year_match='Pass'
                                                    else:
                                                        Release_year_match='Fail'
                                            px_mapping="http://34.231.212.186:81/projectx/%d/mapping/"%search_px_id1_[0]
                                            response_mapping=urllib2.Request(px_mapping)
                                            response_mapping.add_header('Authorization',Token)
                                            resp_mapping=urllib2.urlopen(response_mapping)
                                            data_mapping=resp_mapping.read()
                                            data_resp_mapping=json.loads(data_mapping) 
                                            for pp in data_resp_mapping:
                                                if pp.get("sub_type")=="MO" and pp.get("type")=='Program' and pp.get("data_source")=='GuideBox' :
                                                    arr_gb.append(pp.get("source_id"))
                                                if pp.get("type")=='Program' and pp.get("data_source")=='Rovi' :
                                                    arr_rovi.append(pp.get("source_id"))
                                            if arr_gb:
                                                if str(arr_gb[0])==Gb_id:
                                                    Guidebox_id_match="True"
                                                else:
                                                    Guidebox_id_match="False"
                                            else:
                                                Guidebox_id_match=""
                                        writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":movie_title,"Px_release_year":release_year,"Rovi_id":arr_rovi,"projectx_id":search_px_id1_[0],"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'All link or any of them is present in projectx API',"movie_title_match":movie_title_match,"title_match":title_match,"Release_year_match":Release_year_match,"Guidebox_id_match":Guidebox_id_match})
                                    else:
                                        writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":'',"Px_release_year":'',"Rovi_id":arr_rovi,"projectx_id":sec_arr,"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'Multiple projectx ids found ,None of ids duplicate present in search api ',"movie_title_match":'',"title_match":'',"Release_year_match":'',"Guidebox_id_match":''})         
                            else:
                                writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":'',"Px_release_year":'',"Rovi_id":arr_rovi,"projectx_id":sec_arr,"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'Multiple projectx ids found , search api has no response',"movie_title_match":'',"title_match":'',"Release_year_match":'',"Guidebox_id_match":''}) 
                    elif amazon_flag=='' and starz_flag=='' and netflix_flag=='' and cbs_flag=='' and vudu_flag=='' and itunes_flag=='':
                        writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":'',"Px_release_year":'',"Rovi_id":'',"projectx_id":'',"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"comment":'this links not in the sheet',"movie_title_match":'',"title_match":'',"Release_year_match":'',"Guidebox_id_match":''}) 
                    elif amazon_flag_expired=='False' and vudu_flag_expired=='False' and starz_flag_expired=='False' and netflix_flag_expired=='False' and cbs_flag_expired=='False' and itunes_flag_expired=='False':

                        writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":'',"Px_release_year":'',"Rovi_id":'',"projectx_id":'',"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"amazon_flag_expired":amazon_flag_expired,"vudu_flag_expired":vudu_flag_expired,"starz_flag_expired":starz_flag_expired,"netflix_flag_expired":netflix_flag_expired,"cbs_flag_expired":cbs_flag_expired,"itunes_flag_expired":itunes_flag_expired,"comment":'this links not expired',"movie_title_match":'',"title_match":'',"Release_year_match":'',"Guidebox_id_match":''})
                    else:
                        link=[]
                        link_present=''

                        gb_api="http://34.231.212.186:81/projectx/guideboxdata?sourceId=%d&showType=MO"%eval(Gb_id)
                        response_gb=urllib2.Request(gb_api)
                        response_gb.add_header('Authorization',Token)
                        resp_gb=urllib2.urlopen(response_gb)
                        data_gb=resp_gb.read()
                        data_resp_gb=json.loads(data_gb)
                        if data_resp_gb.get("tv_everywhere_web_sources") or data_resp_gb.get("subscription_web_sources") or data_resp_gb.get("free_web_sources") or data_resp_gb.get("purchase_web_sources") :
                            if data_resp_gb.get("tv_everywhere_web_sources"):
                                for aa in data_resp_gb.get("tv_everywhere_web_sources"):
                                    link.append(aa.get('link'))
                            if data_resp_gb.get("subscription_web_sources"):
                                for aa in data_resp_gb.get("subscription_web_sources"):
                                    link.append(aa.get('link'))
                            if data_resp_gb.get("free_web_sources"):
                                for aa in data_resp_gb.get("free_web_sources"):
                                    link.append(aa.get('link'))
                            if data_resp_gb.get("purchase_web_sources"):
                                for aa in data_resp_gb.get("purchase_web_sources"):
                                    link.append(aa.get('link'))
                            if source_amazon[0]==0:
                               source_amazon[0]=' '
                            if source_starz[0]==0:
                               source_starz[0]=' '
                            if source_netflix[0]==0:
                               source_netflix[0]=' '
                            if source_cbs[0]==0:
                               source_cbs[0]=' '
                            if source_vudu[0]==0:
                               source_vudu[0]=' '
                            if source_itunes[0]==0:
                               source_itunes[0]=' '
 
                            for bb in link:
                                if str(source_amazon[0]) in bb or str(source_starz[0]) in bb or str(source_netflix[0]) in bb or str(source_cbs[0]) in bb or str(source_vudu[0]) in bb or str(source_itunes[0]) in bb:
                                    link_present='True'
                                    break

                                else:
                                    link_present='False'

                            if link_present=='True':     
                                writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":'',"Px_release_year":'',"Rovi_id":'',"projectx_id":'',"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"amazon_flag_expired":amazon_flag_expired,"vudu_flag_expired":vudu_flag_expired,"starz_flag_expired":starz_flag_expired,"netflix_flag_expired":netflix_flag_expired,"cbs_flag_expired":cbs_flag_expired,"itunes_flag_expired":itunes_flag_expired,"comment":'this link not ingested but ott link present in db',"movie_title_match":'',"title_match":'',"Release_year_match":'',"Guidebox_id_match":''})  
                                    
                                        
                            else:
                                writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":'',"Px_release_year":'',"Rovi_id":'',"projectx_id":'',"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"amazon_flag_expired":amazon_flag_expired,"vudu_flag_expired":vudu_flag_expired,"starz_flag_expired":starz_flag_expired,"netflix_flag_expired":netflix_flag_expired,"cbs_flag_expired":cbs_flag_expired,"itunes_flag_expired":itunes_flag_expired,"comment":'this link not ingested and not present in DB',"movie_title_match":'',"title_match":'',"Release_year_match":'',"Guidebox_id_match":''})
                        else:
                            writer.writerow({"Gb_id":Gb_id,"Title":Title,"ReleaseYear":ReleaseYear,"Gb_id_PX":arr_gb,"Search":Search,"Match":Match,"AmazonLink":AmazonLink,"Amazon_Flag":Amazon_Flag,"StarzLink":StarzLink,"Starz_Flag":Starz_Flag,"NetflixLink":NetflixLink,"Netflix_flag":Netflix_flag,"NBCLink":NBCLink,"NBC_flag":NBC_flag,"CBSLink":CBSLink,"CBS_flag":CBS_flag,"VUDULink":VUDULink,"VUDU_flag":VUDU_flag,"ITUNESLink":ITUNESLink,"ITUNES_flag":ITUNES_flag,"Ott_flag":Ott_flag,"Result":Result,"Px_title":'',"Px_release_year":'',"Rovi_id":'',"projectx_id":'',"amazon_flag":amazon_flag,"starz_flag":starz_flag,"netflix_flag":netflix_flag,"cbs_flag":cbs_flag,"vudu_flag":vudu_flag,"itunes_flag":itunes_flag,"amazon_flag_expired":amazon_flag_expired,"vudu_flag_expired":vudu_flag_expired,"starz_flag_expired":starz_flag_expired,"netflix_flag_expired":netflix_flag_expired,"cbs_flag_expired":cbs_flag_expired,"itunes_flag_expired":itunes_flag_expired,"comment":'this link not ingested and not present in DB',"movie_title_match":'',"title_match":'',"Release_year_match":'',"Guidebox_id_match":''})

                    print datetime.datetime.now()


                except httplib.BadStatusLine:
                    print ("exception caught httplib.BadStatusLine.............Retrying..............................")
                    continue
                except urllib2.HTTPError:
                    print ("exception caught HTTPError.................................Retrying..........")
                    continue
                except socket.error:
                    print ("exception caught SocketError......................................Retrying.....")
                    continue
                except URLError:
                    print ("exception caught URLError.......................Retrying....................")
                    continue


t1 =threading.Thread(target=checking_unmapped,args=(1,"thread - 1",19001,1))
t1.start()
t2 =threading.Thread(target=checking_unmapped,args=(19001,"thread - 2",29511,2))
t2.start()

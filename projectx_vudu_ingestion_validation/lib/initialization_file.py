import sys 
import sys
import pymongo

source='Vudu' 
connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
mydb=connection["qadb"]
sourceidtable=mydb["vududump"]

fieldnames = ["%s_id"%source,"show_type","title","release_year","series_title","episode_number","language","projectx_id_%s"%source,"Comment",
                                                                    "duplicate_response_present","duplicate_%s_id"%source,"px_response"]

not_ingested_count=0
multiple_mapped_count=0
pass_count=0
px_response='True'
                                                                    
token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%s&sourceName=Vudu&showType=%s"
duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%s&sourceName=%s&showType=%s'
px_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'
token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'

projectx_preprod_domain='https://preprod.caavo.com/programs?ids=%d&ott=true&aliases=true'



import sys 
import os
import pymongo

source='Hulu'
connection=pymongo.MongoClient("mongodb://192.168.86.12:27017/")
mydb=connection["qadb"] 
mytable_movies=mydb["HuluValidMovies"]
mytable_episodes=mydb["HuluValidEpisodes"]

total_mo=0
total_se=0
total_sm=0
count_valid_programs=0
Not_valid_programs_count=0
count_hulu_id_se=0
count_hulu_id_mo=0

token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'

source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=%s&showType=%s"

projectx_preprod_domain='https://preprod.caavo.com/programs?ids=%d&ott=true&aliases=true'

projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'

gb_image_api='http://34.231.212.186:81/projectx/%d/guideboximage?showType=%s'

rovi_image_api='http://34.231.212.186:81/projectx/%d/roviimage'
projectx_duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'


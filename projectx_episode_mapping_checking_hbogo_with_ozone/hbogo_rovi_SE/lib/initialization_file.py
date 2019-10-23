import sys
import os


source='HBOGO'
total=0

mapped_count=0
not_mapped_count=0
multiple_mapped_count=0
rovi_id_not_ingested_count=0
source_id_not_ingested_count=0
both_source_not_ingested_count=0

projectx_preprod_api="https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true"

rovi_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=Rovi"

projectx_preprod_api_episodes="http://preprod.caavo.com/programs/%d/episodes"

Token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'
projectx_mapping_api_domain='http://34.231.212.186:81/projectx/%d/mapping'

api_duplicate_checking_sm="http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=SM"

source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=%s&showType=%s"

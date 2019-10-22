import sys 
import os

source='Showtime'

total=0
count_showtime_id=0

token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'

source_mapping_api="http://34.231.212.186:81/projectx/mappingfromsource?sourceIds=%d&sourceName=%s&showType=%s"

projectx_preprod_domain='https://preprod.caavo.com/programs?ids=%d&ott=true&aliases=true'

projectx_mapping_api='http://34.231.212.186:81/projectx/%d/mapping/'

gb_image_api='http://34.231.212.186:81/projectx/%d/guideboximage?showType=%s'

rovi_image_api='http://34.231.212.186:81/projectx/%d/roviimage'

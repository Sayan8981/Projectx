
import os
import sys

source='HBOGO'

Token='Token token=0b4af23eaf275daaf41c7e57749532f128660ec3befa0ff3aee94636e86a43e7'
domain_name="https://preprod.caavo.com"
token='Token token=efeb15f572641809acbc0c26c9c1b63f4f7f1fd7dcb68070e45e26f3a40ec8e3'

preprod_version_expire_api='https://test.caavo.com/expired_ott/source_program_id/is_available?source_program_id=%s&service_short_name=%s'
reverse_api_domain='http://34.231.212.186:81/projectx/%s/%s/ottprojectx'
projectx_preprod_search_api='https://preprod.caavo.com/v3/voice_search?q=%s&safe_search=false&credit_summary=true&credit_types=Actor&aliases=true&ott=true'
projectx_preprod_api='https://preprod.caavo.com/programs?ids=%s&ott=true&aliases=true'
preprod_beta_api='https://test.caavo.com/programs?ids=%s?ott=true'
duplicate_api='http://34.231.212.186:81/projectx/duplicate?sourceId=%d&sourceName=%s&showType=%s'
projectx_mapping_api="http://34.231.212.186:81/projectx/%d/mapping/"
credit_db_api="http://34.231.212.186:81/projectx/%d/credits/"

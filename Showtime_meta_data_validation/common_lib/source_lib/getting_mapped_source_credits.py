"""Writer: Saayan"""

import os
import sys
import unidecode
import pinyin


def getting_credits(details,id_):
    #import pdb;pdb.set_trace()
    cast_details=[]
    if details[id_][7] is not None or details[id_][8] is not None:
        cast_details.append(unidecode.unidecode(pinyin.get((details[id_])[7])))
        cast_details.append(unidecode.unidecode(pinyin.get((details[id_])[8]))) 
    return cast_details        
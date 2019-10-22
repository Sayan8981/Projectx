"""Writer: Saayan"""


import os
import sys
import unidecode
import pinyin

def getting_px_credits(data_credits):
    #import pdb;pdb.set_trace()
    px_credits=[]

    for credits in data_credits:
        px_credits.append(unidecode.unidecode(pinyin.get(credits.get("full_credit_name"))))
    return px_credits   
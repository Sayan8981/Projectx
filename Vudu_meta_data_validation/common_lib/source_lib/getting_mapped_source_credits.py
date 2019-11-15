"""Writer: Saayan"""

import os
import sys
import unidecode
import pinyin


def getting_credits(details):
    #import pdb;pdb.set_trace()
    cast_details=[]
    if details.get("credit_summary")!=[] and details.get("credit_summary") is not None:
        
        for cc in details.get("credit_summary"):
            if unidecode.unidecode(pinyin.get(cc.keys()[0]))!='Executive Producer' or unidecode.unidecode(pinyin.get(cc.keys()[0]))!="Producer":
                cast_details.append(unidecode.unidecode(pinyin.get(cc.get(cc.keys()[0]))))

    return cast_details        
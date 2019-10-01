"""Writer: Saayan"""

import sys
import os
import unidecode
import pinyin

def px_aliases(data_aliases,source):
    #import pdb;pdb.set_trace()
    px_aliases=[]
    for aliases in data_aliases:
        if aliases.get("type")=='alias' and aliases.get("source_name")==source:
            px_aliases.append(unidecode.unidecode(pinyin.get(aliases.get("alias"))))
    return px_aliases       
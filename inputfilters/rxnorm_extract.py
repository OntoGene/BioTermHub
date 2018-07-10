#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016
#         Nico Colic, 2018


'''
Parse RxNorm Current Prescribable Content ("RXNCONSO.RFF").
'''

# https://www.nlm.nih.gov/research/umls/rxnorm/docs/2018/rxnorm_doco_full_2018-1.html#s12_4

# RXNSAT.RFF : 
# RXNREL.RFF : atoms and concept links, links RXAUI and RXCUI 


import io
import csv
import sys
import re

DEFAULT_PATH = '/Users/tabris/Downloads/RxNorm_full_prescribe_07022018/rrf/RXNCONSO.RRF'

DEFAULT_OUT = '/Users/tabris/Downloads/terms.txt'

def main():
    if len(sys.argv) > 1:
        terms = load(sys.argv[1])
    else:
        terms = load(DEFAULT_PATH)
        
    inverted = invert(terms)
    
    write(inverted)
        

def load(path):
    pack_regex = re.compile('\[(?P<name>.*?)\]')
    quantity_regex = re.compile('(.*) \d+(\.|,)?\d* (mg|MG|mg/ml|MG/ML|unt|UNT|unt/ml|UNT/ML|\[iu\]|\[IU\]|gm|GM|%) ')
    
    terms = dict()

    with open(path,'r') as f:
        csvreader = csv.reader(f, delimiter="|")
        for row in csvreader:
            
            add_to_terms = []
            
            # is it a pack?
            pack_matches = pack_regex.findall(row[14])
            if pack_matches:
                add_to_terms.extend(pack_matches)
                
            # does it include quantities?
            term = quantity_regex.search(row[14])
            if term:
                add_to_terms.extend([term.group(1)])
            else:
                add_to_terms.extend([row[14]])
            
            # wrap up    
            if len(add_to_terms) > 0:
                if row[0] not in terms:
                    terms[row[0]] = add_to_terms
                else:
                    terms[row[0]].extend(add_to_terms)
        
        for k,v in terms.items():
            v_ = sorted(set([term.lower() for term in v]))
            terms[k] = v_
            
    return terms
    
def invert(terms):
    
    inverted = dict()
    for k,value in terms.items():
        for v in value:
            if v not in inverted:
                inverted[v] = [k]
            else:
                inverted[v].append(k)
            
    return inverted
    
def write(terms, print_id=False, out=DEFAULT_OUT):
    with open(out, 'w') as f:
        for term,value in sorted(terms.items()):
            if print_id:
                f.write(term + ' : ' + str(value) + '\n')
            else:
                f.write(term + '\n')

    

if __name__ == '__main__':
    main()

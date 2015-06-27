#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Python Program Description: Script for ....

"""
from optparse import OptionParser
import codecs
import sys
import os
#from Bio import Entrez
import codecs
import shutil
import random
import nltk
import csv
import oid_generator

import collections
from StringIO import StringIO


from Orange.bio.ontology import OBOOntology
from Orange.bio.ontology import OBOParser


sys.path.append('../lib')
from unicode_csv import UnicodeDictWriter


# Prevent Encoding exceptions in Python 2.x
sys.stdout = codecs.getwriter('utf-8')(sys.__stdout__)
sys.stderr = codecs.getwriter('utf-8')(sys.__stderr__)
sys.stdin = codecs.getreader('utf-8')(sys.__stdin__)



def parse_obo(chebi_file):
    chebif = open(chebi_file, 'r')
    
    chebi_text = chebif.read()
    
    file = StringIO(chebi_text)
    
    parser = OBOParser(file)
    
    for event, value in parser:
        
        if event == 'START_STANZA':
            
            term_dict = {}
            term_dict['synonym_list'] = []
            
        elif event == 'TAG_VALUE':
            tag = value[0]

            if tag == 'name':
                term_dict['term'] = value[1]
            elif tag == 'id':
                term_dict['id'] = value[1]
            elif tag == 'synonym':
                synonym = value[1].split('"')[1]
                ##print value, 'VAL'
                ##print synonym, 'SYN'
                term_dict['synonym_list'].append(synonym)
            else: pass
            
        elif event == 'CLOSE_STANZA':
            yield term_dict
        
def obodict2ontogene_headers(dict_list):

    for term_dict in dict_list:
        onto_dict = {}
        onto_dict['preferred_term'] = term_dict['term']
        onto_dict['original_id'] = term_dict['id']
        onto_dict['entity_type'] = 'chemical'
        onto_dict['resource'] = 'CHEBI'
        onto_dict['oid'] = oid_generator.OID.get()

        if term_dict['synonym_list']:
            for syn in term_dict['synonym_list']:
                onto_dict_cp = onto_dict.copy()
                onto_dict_cp['term'] = syn
                yield onto_dict_cp, False

        onto_dict['term'] = term_dict['term']
        yield onto_dict, True

def dict_to_file(dict_list, output_file):
    ''' takes a list of dictionaries and writes it to a csv file'''

    csv_file = codecs.open(output_file, 'wt', 'utf-8')

    fieldnames = ['original_id', 'term', 'entity_type','preferred_term', 'resource', 'oid']

    try:

        writer = UnicodeDictWriter(csv_file, dialect= csv.excel_tab, fieldnames=fieldnames, quotechar=str("\""), quoting= csv.QUOTE_NONE, restval='__')

        headers = dict( (n,n) for n in fieldnames )

        for one_dict in dict_list:
            writer.writerow(one_dict)

    finally: csv_file.close()

def process(options=None, args=None):
    """
    Do the processing.

    The options object should be used as an argument to almost all functions.
    This gives easy access to all global parameters.
    """
    input_file = args[0]

    output_file = args[1]
    
    obo_dict_list = parse_obo(input_file)
    
    output_dict_list, _ = obodict2ontogene_headers(obo_dict_list)
    
    dict_to_file(output_dict_list, output_file)

def main():
    """
    Invoke this module as a script
    """
    usage = "usage: %prog [options] input: directory of pubmed dump with abstract data(args[0])"
    parser = OptionParser(version='%prog 0.99', usage=usage)

    parser.add_option('-l', '--logfile', dest='logfilename',
                      help='write log to FILE', metavar='FILE')
    parser.add_option('-q', '--quiet',
                      action='store_true', dest='quiet', default=False,
                      help='do not #print status messages to stderr')
    parser.add_option('-d', '--debug',
                      action='store_true', dest='debug', default=False,
                      help='#print debug information')



    (options, args) = parser.parse_args()

    #if options.debug: #print >> sys.stderr, '# Starting processing'

    process(options=options,args=args)




    sys.exit(0) # Everything went ok!

if __name__ == '__main__':
    main()

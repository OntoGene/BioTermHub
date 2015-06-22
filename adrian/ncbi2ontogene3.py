#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Tilia Ellendorff
# Fork: adrian.vanderlek@uzh.ch

"""
Python Program Description: Script for converting a database (ncbi gene_info) into the ontogene term matcher format. Original program: gene_families3.py

"""
from optparse import OptionParser
import codecs
import sys
import os
import sys
from oid_generator import OID
sys.path.insert(0, '../lib')

#from Bio import Entrez
import codecs
import shutil
import random
import nltk
import collections
import csv
import re

import difflib
from unicode_csv import UnicodeCsvReader, UnicodeDictReader, UnicodeDictWriter

def process_file(csv_file, options=None, args=None):
    '''Reads a csv file and produces a list of dictionaries with one dictionary per line using UnicodeDictReader; Headers are used as keys.'''

    # Generate proper header from first line
    infile = codecs.open(csv_file, 'r')
    
    reader = UnicodeDictReader(infile, dialect=csv.excel_tab, quoting=csv.QUOTE_NONE, quotechar=str("\"")) 

    if options.id_list:
        #Only applies if a list of ids is given; only take ids in list into account 
        
        id_list_file = open(options.id_list, 'r')
        id_list = [unicode(id.rstrip('\n')) for id in id_list_file.readlines()]
        # print 'id_list'
#         print id_list


        file_list = []

        for row in reader:
            #print row
            gene_id = row['GeneID']
            #print gene_id
            if gene_id in id_list:
                file_list.append(row)

        #file_list = [row for row in reader if row['GeneID'] in id_list]

    else: 
        file_list = [row for row in reader]
        #Use all lines of the file
        
        #print sys.stderr,'#',file_list[0]

    return file_list


def dict_to_file(dict_list, output_file):
    ''' Takes a list of dictionaries and writes it to a csv file using UnicodeDictWriter'''

    csv_file = codecs.open(output_file, 'wt', 'utf-8')

    fieldnames = ['ncbi_id', 'term', 'type','reference']
    #the names of the headers

    print >> sys.stderr, '# STATUS: fieldnames', " ".join(fieldnames)


    try:

        writer = UnicodeDictWriter(csv_file, dialect= csv.excel_tab, fieldnames=fieldnames, quotechar=str("\""), quoting= csv.QUOTE_NONE, restval='__')

        headers = dict( (n,n) for n in fieldnames )


        #writer.writerow(headers)
        #(only use if headers are wanted)

        for one_dict in dict_list:
            writer.writerow(one_dict)

    finally: csv_file.close()



def transform_input(file_list, mapping = None, unified_build = False):
    '''Processes the input in the following ways:
    - Splits lists of synonyms and converts them to different entries; 
    - Processes 'descriptions' fields and 'other designations' fields and finds longest common substrings (additional terms) for high recall (experimental, can be removed)'''

    new_file_list = []
    
    if unified_build:
        term_count = len(file_list)
        id_count = 0

    for line_dict in file_list:
        
        if unified_build:
            current_oid = OID.get()

        term_list = []

        synonyms = line_dict['Synonyms']
        # dbXrefs = line_dict['dbXrefs']

    # Add Symbol itself to term list
        term_list.append(line_dict['Symbol'])

    # Split "Synonyms", "description" and "Other_designations" fields at '|' (and '/')
        if not synonyms == '-':
            synonym_list = synonyms.replace('/', '|').split('|')
            term_list.extend(synonym_list)
        #if not dbXrefs == '-':
        #    dbXrefs_list = ...
        #    term_list.extend(dbXrefs_list)

        # print 'TERM LIST', term_list

        if not term_list == []:

            for term in list(set(term_list)):
                output_dict = {}
                id_count += 1
                if not unified_build:
                    output_dict['term'] = term
                    output_dict['type'] = 'gene'
                    output_dict['ncbi_id'] = line_dict['GeneID']
                    output_dict['reference'] = line_dict['Symbol']
                else:
                    output_dict[mapping['term']] = term
                    output_dict[mapping['type']] = 'gene'
                    output_dict[mapping['ncbi_id']] = line_dict['GeneID']
                    output_dict[mapping['reference']] = line_dict['Symbol']
                    output_dict["oid"] = current_oid
                    output_dict["resource"] = "Entrezgene"

                # print 'OUTPUT_DICT', output_dict

                new_file_list.append(output_dict)
                
    if not unified_build:
        return new_file_list
    else:
        return new_file_list, term_count, id_count

def process(options=None, args=None):
    """
    Do the processing.

    Put together all functions.
    """
    #if options.debug:
    #    print >>sys.stderr, options

    #print sys.stdin, 'test'

    print 'OPTIONS:', options

    input_file = args[0]

    output_file = args[1]

    processed_input = process_file(input_file, options=options, args=args)
    # Read input file; (sort out input if list of ids is given); return dictionary structure

    #print processed_input, 'INPUT DICT'

    new_file_list = transform_input(processed_input)
    # Transform input into separate dictionaries for synonyms. 
    # The function transform_input will have to be adapted


    dict_to_file(new_file_list, output_file)
    # Write the new list of dictionaries to the output file

    print len(new_file_list), 'LINES WERE PRODUCED'



def main():
    """
    Invoke this module as a script
    """
    usage = "usage: %prog [options]; args[0]: original (database) file (NCBI); args[1]: output csv file ('Ontogene Format')"
    parser = OptionParser(version='%prog 0.99', usage=usage)

    parser.add_option('-l', '--logfile', dest='logfilename',
                      help='write log to FILE', metavar='FILE')
    parser.add_option('-q', '--quiet',
                      action='store_true', dest='quiet', default=False,
                      help='do not print status messages to stderr')
    parser.add_option('-d', '--debug',
                      action='store_true', dest='debug', default=False,
                      help='print debug information')

    parser.add_option('-i', '--id_list', action="store", type="string", dest='id_list', default=False,
                      help='only keep gene information for genes from an external list', metavar="FILE")




    (options, args) = parser.parse_args()

    if options.debug: print >> sys.stderr, '# Starting processing'

    process(options=options,args=args)




    sys.exit(0) # Everything went ok!

if __name__ == '__main__':
    main()

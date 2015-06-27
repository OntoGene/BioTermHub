#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Tilia Ellendorff


"""
Python Program Description: Script for calculation term statistics over a term file in the ontogene term matcher format.

"""
from optparse import OptionParser
import codecs
import sys
import os
import sys

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


class ResourceStats(object):
    def __init__(self, resource_name):
    
        ###############################################
        # PER RESOURCE
    
        self.resource = resource_name
    
        self.ids_dict = {}
        # counts number of occurrences of ids per original resource
        # Key: ID, Value: counter
    
        self.terms_dict = {}
        # counts number of occurrences of term per original resource
        # Key: term, Value: counter
    
        self.terms_total = len(self.terms_dict)
        # calculates total number of terms per original resource (types)
        # IS THIS UPDATED?
    
        self.ids_total = len(self.ids_dict)
        # calculates number of ids per original resource (types)
        # IS THIS UPDATED?
    
        self.ambiguous_term_dict = {}
        # counts ids per term type for each resource (how many different ids are associated to one term (types)?)
        # Key: term, Value: list of ids
    
        self.synonyms_dict = {}
        # counts number of synonyms for each id for each resource ("ambiguous ids")
        # Key: ID, Value: list of terms
    
    def update_resource_stats(self, line_dict):
    
        one_id = line_dict['original_id']
        one_term = line_dict['term']
        
        if not one_id in self.ids_dict:
            self.ids_dict[one_id] = 1
        else: self.ids_dict += 1
        
        if not one_term in self.terms_dict:
            self.terms_dict[one_term] = 1
        else: self.terms_dict[one_term] += 1
        
        if not one_term in self.ambiguous_term_dict:
            self.ambiguous_term_dict[one_term] = set([one_id])
        else: self.ambiguous_term_dict[one_term].add(one_id)
        
        if not one_id in self.synonyms_dict:
            self.synonyms_dict[one_id] = set([one_term])
        else: self.synonyms_dict[one_id].add(one_term)
            
        
        


class OverallStats(object):
    def __init__(self):
        #fieldnames = 'oid','resource', 'original_id', 'term', 'preferred_term', 'entity_type'
    
        # WHOLE TERM FILE
    
        self.all_lines_counter = 0
        # counts the total number of entries in the whole term file (tokens)
        # simple counter
    
        self.resource_dict = {}
        # stores ResourceStats objects for all resources
    
    
    
        self.terms_total_types_dict = {}
        # counts the total number of terms (unique) in the whole term file
        # Key: Term, Value: counter
    
        self.terms_total = len(self.terms_total_types_dict)
        # calculates total number of terms (types)
        # IS THIS UPDATED?
    
        self.ambiguous_terms = {}
        # counts number of ids per terms (unique) in the whole term file
        # Key: Term, Value: set of ids
    
        # (ambiguous ids/synonyms does not make sense here, as they can only be calculated per id == per original resource (see above))
    
        self.ambiguous_terms_lower = {}
        # counts number of ids per term (unique) in the whole term file with all terms lowercased
        # Key: Term, Value: set of ids
    
        self.ambiguous_terms_nows = {}
        # counts number of ids per terms (unique) in the whole term file with all terms lowercased and all non-alphabetical chars removed
        # Key: Term, Value: set of ids
    

    def update_stats(self, line_dict):
        self.all_lines_counter += 1
    
        resource = line_dict['resource']
        
        # Update Resource Stats
        if not resource in self.resource_dict:
            resource_stats = ResourceStats(resource)
            self.resource_dict[resource] = resource_stats
            resource_stats.update_resource_stats(line_dict)
            
        one_term = line_dict['term']
        one_id = line_dict['original_id']
        one_term_lw = one_term.lower()
        one_term_nws = re.sub(r'([^\w]|_)+','', one_term)
        # removes all non-alphabetical characters and whitespace
        
        if not one_term in self.terms_total_types_dict:
            self.terms_total_types_dict['term'] = 1
        else: self.terms_total_types_dict['term'] += 1
        
        if not one_term in self.ambiguous_terms:
            self.ambiguous_terms[one_term] = set([one_id])
        else: self.ambiguous_terms[one_term].add(one_id)
        
        if not one_term_lw in self.ambiguous_terms_lower:
            self.ambiguous_terms_lower[one_term_lw] = set([one_id])
        else: self.ambiguous_terms_lower[one_term_lw].add(one_id)
        
        if not one_term_nws in self.ambiguous_terms_nows:
            self.ambiguous_terms_nows[one_term_nws] = set([one_id])
        else: self.ambiguous_terms_nows[one_term_nws].add(one_id)
        
    def calculate_dict_avg(self, one_dict):
        total_count = 0
        for entry, count in one_dict.items():
            total_count += count
            
        avg = float(total_count)/float(len(one_dict))
        
        return avg
        
        
        
    def display_stats(self):
        print 'Number of lines/terms:', self.all_lines_counter
        print 'Resources:', self.resource_dict.keys()
        print 'Number Resources:', len(self.resource_dict)
        print 'Total number of unique terms (types) in the term file:', len(self.terms_total_types_dict)
        print 'Average of tokens per type:', self.calculate_dict_avg(self.terms_total_types_dict)
        
        
        
        
            


def process_file(csv_file, options=None, args=None):
    '''Reads a csv file and produces a list of dictionaries with one dictionary per line using UnicodeDictReader; Headers are used as keys.'''

    # Generate proper header from first line
    infile = codecs.open(csv_file, 'r')
    
    reader = UnicodeDictReader(infile, dialect=csv.excel_tab, quoting=csv.QUOTE_NONE, quotechar=str("\"")) 
    print reader
    
    
    #fieldnames = 'oid','resource', 'original_id', 'term', 'preferred_term', 'entity_type'
    
    overall_stats = OverallStats()

    for row in reader:
        #file_list.append(row)
        overall_stats.update_stats(row)
        
    overall_stats.display_stats()



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

    #output_file = args[1]

    processed_input = process_file(input_file, options=options, args=args)
    # Read input file and calculate statistics



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



    (options, args) = parser.parse_args()

    if options.debug: print >> sys.stderr, '# Starting processing'

    process(options=options,args=args)




    sys.exit(0) # Everything went ok!
    
if __name__ == '__main__':
    main()

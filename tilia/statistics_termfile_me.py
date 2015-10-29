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
import cProfile
import collections
import numpy as np

import difflib
from unicode_csv import UnicodeCsvReader, UnicodeDictReader, UnicodeDictWriter

class EntityTypeStats(object):
    def __init__(self, entity_type):
    
        ###############################################
        # PER Entity Type
    
        self.entity_type = entity_type
    
        self.ids_dict = {}
        # counts number of occurrences of ids per entity type
        # Key: ID, Value: counter
        # included: calculates number of ids per original entity type (types) = length of ids_dict
    
        self.terms_dict = {}
        # counts number of occurrences of term per original entity type
        # Key: term, Value: counter
        # included: calculate total number of terms per original entity type (types) = length of term_dict
        
        self.synonyms_dict = {}
        # counts number of synonyms for each id for each entity type ("ambiguous ids")
        # Key: ID, Value: list of terms
    
        self.ambiguous_term_dict = {}
        # counts ids per term type for each entity type (how many different ids are associated to one term (types)?)
        # Key: term, Value: list of ids
        
        self.ambiguous_terms_lower = {}
        # counts number of ids per term (unique) in the whole term file with all terms lowercased
        # Key: Term, Value: set of ids
    
        self.ambiguous_terms_nows = {}
        # counts number of ids per terms (unique) in the whole term file with all terms lowercased and all non-alphabetical chars removed
        # Key: Term, Value: set of ids
    
        
    def update_entity_type_stats(self, line_dict):
    
        one_id = line_dict['original_id']
        one_term = line_dict['term']
        one_term_lw = one_term.lower()
        one_term_nws = re.sub(r'([^\w]|_)+','', one_term_lw)
        # removes all non-alphabetical characters and whitespace from lowercase term
        
        if not one_id in self.ids_dict:
            self.ids_dict[one_id] = 1
        else: self.ids_dict[one_id] += 1
        
        if not one_term in self.terms_dict:
            self.terms_dict[one_term] = 1
        else: self.terms_dict[one_term] += 1
        
        if not one_term in self.ambiguous_term_dict:
            self.ambiguous_term_dict[one_term] = np.array([one_id])
        else: np.append(self.ambiguous_term_dict[one_term], one_id)
        
        if not one_term_lw in self.ambiguous_terms_lower:
            self.ambiguous_terms_lower[one_term_lw] = np.array([one_id])
        else: np.append(self.ambiguous_terms_lower[one_term_lw], one_id)
        
        if not one_term_nws in self.ambiguous_terms_nows:
            self.ambiguous_terms_nows[one_term_nws] = np.array([one_id])
        else: np.append(self.ambiguous_terms_nows[one_term_nws], one_id)
        
        if not one_id in self.synonyms_dict:
            self.synonyms_dict[one_id] = np.array([one_term])
        else: np.append(self.synonyms_dict[one_id], one_term)
        
        
    def calculate_dict_avg(self, one_dict):
        total_count = 0
        
        for entry, count in one_dict.items():
            if type(count) is int:
                total_count += count
            if isinstance(count, np.ndarray):
				count_unique = np.unique(count)
				total_count += len(count_unique)
            
        avg = float(total_count)/float(len(one_dict))
        
        return avg
        
    def calculate_dict_freq_dist(self, one_dict):
        freq_list = [len(set(one_list)) for id, one_list in one_dict.items()]
        
        freq_dict = collections.Counter(freq_list)
        
        return freq_dict
        
        
    def id_freq_dist(self):
        # gives a frequency distribution of number of terms per id ("synonyms")
        
        term_freq_dict = self.calculate_dict_freq_dist(self.synonyms_dict)
        return term_freq_dict
        
        
    def term_freq_dist(self):    
        # gives a frequency distribution of number of ids per term ("ambiguous terms")
        
        id_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_term_dict)
        return id_freq_dict
        
        
    def term_lw_freq_dist(self):
        term_lw_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_terms_lower)
        return term_lw_freq_dict
        
    def term_lw_nows_freq_dist(self):
        term_lw_nows_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_terms_nows)
        return term_lw_nows_freq_dict
        
            
        
    def display_entity_type_stats(self):
        print '\n'
        
        print 'Entity Type statistics for', self.entity_type
        print 'Number of original IDs:', len(self.ids_dict)
        #print self.ids_dict
        print 'Number or original terms:', len(self.terms_dict)
        print 'Average of IDs associated to one term ("ambiguous terms"):', self.calculate_dict_avg(self.ambiguous_term_dict)
        print 'Average of Terms associated to one ID ("synonyms"):', self.calculate_dict_avg(self.synonyms_dict)
        
        print 'FREQ DIST number of terms per id', self.id_freq_dist()
        
        print 'FREQ DIST number of ids per term', self.term_freq_dist()
        print 'FREQ DIST number of ids per lower-cased term', self.term_lw_freq_dist()
        print 'FREQ DIST number or ids per lower-cased term with non alphabetical characters removed', self.term_lw_nows_freq_dist()
    
        
        
class ResourceStats(object):
    def __init__(self, resource_name):
    
        ###############################################
        # PER RESOURCE
    
        self.resource = resource_name
    
        self.ids_dict = {}
        # counts number of occurrences of ids per original resource
        # Key: ID, Value: counter
        # included: calculates number of ids per original resource (types) = length of ids_dict
    
        self.terms_dict = {}
        # counts number of occurrences of term per original resource
        # Key: term, Value: counter
        # included: calculate total number of terms per original resource (types) = length of term_dict
        
        self.synonyms_dict = {}
        # counts number of synonyms for each id for each resource ("ambiguous ids")
        # Key: ID, Value: list of terms
    
        self.ambiguous_term_dict = {}
        # counts ids per term type for each resource (how many different ids are associated to one term (types)?)
        # Key: term, Value: list of ids
        
        self.ambiguous_terms_lower = {}
        # counts number of ids per term (unique) in the whole term file with all terms lowercased
        # Key: Term, Value: set of ids
    
        self.ambiguous_terms_nows = {}
        # counts number of ids per terms (unique) in the whole term file with all terms lowercased and all non-alphabetical chars removed
        # Key: Term, Value: set of ids
    
    
    def update_resource_stats(self, line_dict):
    
        one_id = line_dict['original_id']
        one_term = line_dict['term']
        one_term_lw = one_term.lower()
        one_term_nws = re.sub(r'([^\w]|_)+','', one_term_lw)
        # removes all non-alphabetical characters and whitespace from lowercase term
        
        if not one_id in self.ids_dict:
            self.ids_dict[one_id] = 1
        else: self.ids_dict[one_id] += 1
        
        if not one_term in self.terms_dict:
            self.terms_dict[one_term] = 1
        else: self.terms_dict[one_term] += 1
        
        if not one_term in self.ambiguous_term_dict:
            self.ambiguous_term_dict[one_term] = np.array([one_id])
        else: np.append(self.ambiguous_term_dict[one_term], one_id)
        
        if not one_term_lw in self.ambiguous_terms_lower:
            self.ambiguous_terms_lower[one_term_lw] = np.array([one_id])
        else: np.append(self.ambiguous_terms_lower[one_term_lw], one_id)
        
        if not one_term_nws in self.ambiguous_terms_nows:
            self.ambiguous_terms_nows[one_term_nws] = np.array([one_id])
        else: np.append(self.ambiguous_terms_nows[one_term_nws], one_id)
            
        if not one_id in self.synonyms_dict:
            self.synonyms_dict[one_id] = np.array([one_term])
        else: np.append(self.synonyms_dict[one_id], one_term)
        
        
    def calculate_dict_avg(self, one_dict):
        total_count = 0
        
        for entry, count in one_dict.items():
            if type(count) is int:
                total_count += count
            if isinstance(count, np.ndarray):
				count_unique = np.unique(count)
				total_count += len(count_unique)
            
        avg = float(total_count)/float(len(one_dict))
        
        return avg
        
    def calculate_dict_freq_dist(self, one_dict):
        freq_list = [len(set(one_list)) for id, one_list in one_dict.items()]
        
        freq_dict = collections.Counter(freq_list)
        
        return freq_dict
     
    def id_freq_dist(self):
        # gives a frequency distribution of number of terms per id ("synonyms")
        
        term_freq_dict = self.calculate_dict_freq_dist(self.synonyms_dict)
        return term_freq_dict
        
    
    def term_freq_dist(self):
        # gives a frequency distribution of number of ids per term ("ambiguous terms")
        
        id_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_term_dict)
        return id_freq_dict
        
        
    
        
        
    def term_lw_freq_dist(self):
        term_lw_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_terms_lower)
        return term_lw_freq_dict
        
    def term_lw_nows_freq_dist(self):
        term_lw_nows_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_terms_nows)
        return term_lw_nows_freq_dict
        
    def display_resource_stats(self):
    
        
        print '\n'
        
        print 'Resource statistics for', self.resource
        print 'Number of original IDs:', len(self.ids_dict)
        #print self.ids_dict
        print 'Number or original terms:', len(self.terms_dict)
        print 'Average of IDs associated to one term ("ambiguous terms"):', self.calculate_dict_avg(self.ambiguous_term_dict)
        print 'Average of Terms associated to one ID ("synonyms"):', self.calculate_dict_avg(self.synonyms_dict)
        
        print 'FREQ DIST number of terms per id', self.id_freq_dist()
        print 'FREQ DIST number of ids per term', self.term_freq_dist()
        
        print 'FREQ DIST number of ids per lower-cased term', self.term_lw_freq_dist()
        print 'FREQ DIST number or ids per lower-cased term with non alphabetical characters removed', self.term_lw_nows_freq_dist()
        


class OverallStats(object):
    def __init__(self):
        #fieldnames = 'oid','resource', 'original_id', 'term', 'preferred_term', 'entity_type'
    
        # WHOLE TERM FILE
    
        self.all_lines_counter = 0
        # counts the total number of entries in the whole term file (tokens)
        # simple counter
    
        self.resource_dict = {}
        # stores ResourceStats objects for all resources
        
        self.entity_type_dict = {}
    
    
    
        self.terms_total_types_dict = {}
        # counts the total number of terms (unique) in the whole term file
        # Key: Term, Value: counter
        
        self.synonyms_dict = {}
        # counts number of ontogene ids (oids) per term in the whole term file
    
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
        entity_type = line_dict['entity_type']
        
        # Update Resource Stats
        if not resource in self.resource_dict:
            resource_stats = ResourceStats(resource)
            self.resource_dict[resource] = resource_stats
            self.resource_dict[resource].update_resource_stats(line_dict)
            #resource_stats.update_resource_stats(line_dict)
        else: self.resource_dict[resource].update_resource_stats(line_dict)
        
        if not entity_type in self.entity_type_dict:
            entity_type_stats = EntityTypeStats(entity_type)
            self.entity_type_dict[entity_type] = entity_type_stats
            self.entity_type_dict[entity_type].update_entity_type_stats(line_dict)
            #resource_stats.update_resource_stats(line_dict)
        else: self.entity_type_dict[entity_type].update_entity_type_stats(line_dict)
            
        one_term = line_dict['term']
        one_id = line_dict['original_id']
        one_oid = line_dict['oid']
        one_term_lw = one_term.lower()
        one_term_nws = re.sub(r'([^\w]|_)+','', one_term_lw)
        # removes all non-alphabetical characters and whitespace from lowercase term
        
        if not one_term in self.terms_total_types_dict:
            self.terms_total_types_dict[one_term] = 1
        else: self.terms_total_types_dict[one_term] += 1
        
        if not one_id in self.synonyms_dict:
            self.synonyms_dict[one_id] = np.array([one_term])
        else: np.append(self.synonyms_dict[one_id], one_term)
        
        if not one_term in self.ambiguous_terms:
            self.ambiguous_terms[one_term] = np.array([one_id])
        else: np.append(self.ambiguous_terms[one_term], one_id)
        
        if not one_term_lw in self.ambiguous_terms_lower:
            self.ambiguous_terms_lower[one_term_lw] = np.array([one_id])
        else: np.append(self.ambiguous_terms_lower[one_term_lw], one_id)
        
        if not one_term_nws in self.ambiguous_terms_nows:
            self.ambiguous_terms_nows[one_term_nws] = np.array([one_id])
        else: np.append(self.ambiguous_terms_nows[one_term_nws], one_id)
        
    def calculate_dict_avg(self, one_dict):
        total_count = 0
        
        for entry, count in one_dict.items():
            if type(count) is int:
                total_count += count
            if isinstance(count, np.ndarray):
				count_unique = np.unique(count)
				total_count += len(count_unique)
            
        avg = float(total_count)/float(len(one_dict))
        
        return avg
        
    def calculate_dict_freq_dist(self, one_dict):
        freq_list = [len(set(one_list)) for id, one_list in one_dict.items()]
        
        freq_dict = collections.Counter(freq_list)
        
        return freq_dict
        
        
    def id_freq_dist(self):
        #gives a frequency distribution for numbers of terms per id (oids for whole resource)
        # without mapping oids correspond to original ids
        
        id_freq_dict = self.calculate_dict_freq_dist(self.synonyms_dict)
        return id_freq_dict
        
    def term_freq_dist(self):
        #gives a frequency distribution for numbers of ids per term (original ids)
        term_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_terms)
        return term_freq_dict
        
    def term_lw_freq_dist(self):
        term_lw_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_terms_lower)
        return term_lw_freq_dict
        
    def term_lw_nows_freq_dist(self):
        term_lw_freq_dict = self.calculate_dict_freq_dist(self.ambiguous_terms_nows)
        return term_lw_freq_dict
      
        
    def display_stats(self):
        print '\n'
        print 'STATS FOR WHOLE TERM FILE'
        print 'Number of lines/terms:', self.all_lines_counter
        print 'Resources:', ', '.join(self.resource_dict.keys())
        print 'Number Resources:', len(self.resource_dict)
        print 'Total number of unique terms (types) in the term file:', len(self.terms_total_types_dict)
        print 'Average of tokens per type:', self.calculate_dict_avg(self.terms_total_types_dict)
        print 'Average of ids per term:', self.calculate_dict_avg(self.ambiguous_terms)
        print 'Average of ids per term with lowercased terms:', self.calculate_dict_avg(self.ambiguous_terms_lower)
        print 'Average of ids per term with lowercased terms and non-alphabetical characters removed:', self.calculate_dict_avg(self.ambiguous_terms_nows)
        
        print 'FREQ DIST number of terms per id', self.id_freq_dist()
        print 'FREQ DIST number of ids per term', self.term_freq_dist()
        print 'FREQ DIST number of ids per term (terms are lowercased)', self.term_lw_freq_dist()
        print 'FREQ DIST number of ids per term (terms are lowercased and non whitespace characters are removed', self.term_lw_nows_freq_dist()
        
        
        print '-----------'
        print 'RESOURCE STATS'
        for resource, resource_stats in self.resource_dict.items():
            resource_stats.display_resource_stats()
            
            #term_freq_dist = resource_stats.term_freq_dist()
             # gives a frequency distribution of number of terms per id ("synonyms")
            #id_freq_dist = resource_stats.id_freq_dist()
            # gives a frequency distribution of number of ids per term ("ambiguous terms")
            
            
        print '-----------'
        print 'ENTITY TYPE STATS'   
        for entity_type, entity_type_stats in self.entity_type_dict.items():
            entity_type_stats.display_entity_type_stats()
            
            #term_freq_dist = entity_type_stats.term_freq_dist()
             # gives a frequency distribution of number of terms per id ("synonyms")
            #id_freq_dist = entity_type_stats.id_freq_dist()
            # gives a frequency distribution of number of ids per term ("ambiguous terms")
            
            
            
        
        
            


def process_file(csv_file, options=None, args=None):
    '''Reads a csv file and produces a list of dictionaries with one dictionary per line using UnicodeDictReader; Headers are used as keys.'''

    # Generate proper header from first line
    infile = codecs.open(csv_file, 'r')
    
    reader = UnicodeDictReader(infile, dialect=csv.excel_tab, quoting=csv.QUOTE_NONE, quotechar=str("\"")) 
    
    
    #fieldnames = 'oid','resource', 'original_id', 'term', 'preferred_term', 'entity_type'
    
    overall_stats = OverallStats()

    for row in reader:
        #file_list.append(row)
        overall_stats.update_stats(row)
        
    overall_stats.display_stats()
    
    
    if not __name__ == '__main__':
        return overall_stats



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
    cProfile.run('main()','calculate_statistics.profile')
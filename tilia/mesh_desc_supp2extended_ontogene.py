#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Python Program Description: Script for ....

"""
from optparse import OptionParser
import codecs
import sys
import os
from Bio import Entrez
import codecs
import shutil
import random
import nltk
import collections
import csv
import cStringIO
import xml.etree.cElementTree as ET


import cProfile
import tstables



# Prevent Encoding exceptions in Python 2.x
sys.stdout = codecs.getwriter('utf-8')(sys.__stdout__)
sys.stderr = codecs.getwriter('utf-8')(sys.__stderr__)
sys.stdin = codecs.getreader('utf-8')(sys.__stdin__)




class UnicodeDictWriter(object):

    def __init__(self, f, fieldnames, dialect=csv.excel_tab, quoting= csv.QUOTE_NONE, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.DictWriter(self.queue, fieldnames, quoting=quoting, escapechar = chr(92),dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, D):
        self.writer.writerow({unicode(k):unicode(v).encode("utf-8") for k,v in D.items()})
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data.decode("utf-8"))
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for D in rows:
            self.writerow(D)

    def writeheader(self):
        self.writer.writeheader()


def parse_desc_file(desc_file):
    '''Parses mesh desc xml file and returns a list (desc_dict_list) of dictionaries (one_child_dict). The dictionaries have the keys 'DescriptorUI', 
    'term set' (contains terms from DescriptorName, TermList und ConceptName), 'TreeNumbers' (contains list of Mesh Tree Numbers which 
    encode the entity type'''


    #relevant_list = ['ConceptName', 'TermList', 'ThesaurusIDlist', 'TreeNumber', 'TreeNumberList', 'SemanticTypeName', 'Term', 'String']
    
    mesh_trees = {'A' : 'Anatomy', 'B' : 'Organisms', 'C' : 'Diseases', 'D' : 'Chemicals and Drugs', 'E' : 'Analytical,Diagnostic and Therapeutic Techniques and Equipment',
    'F' : 'Psychiatry and Psychology', 'G' : 'Phenomena and Processes', 'H' : 'Disciplines and Occupations', 'I' : 'Anthropology,Education,Sociology and Social Phenomena', 'J' : 'Technology,Industry,Agriculture',
    'K' : 'Humanities', 'L' : 'Information Science', 'M' : 'Named Groups', 'N' : 'Health Care', 'V' : 'Publication Characteristics', 
    'Z' : 'Geographicals'}

    input_file = desc_file

    tree = ET.parse(input_file)
    
    desc_tree_dict = {}

    desc_dict_list = []

    root = tree.getroot()

    print len(root)
    
    print root

    tag_list = []

    for i in range(len(root)):

        print i, '-------'
        print 'Parent number', i, ':', root[i]
        one_child_dict = {}
        # one dictionary per xml entry
        
        one_child_dict['term_set'] = set([])
        
        
        for child in root[i]:
                
                
                if child.tag == 'DescriptorUI':
                    #print 'DescriptorUI found:', child.text
                    desc_ui = child.text
                    
                    one_child_dict['DescriptorUI'] = desc_ui
                    desc_tree_dict[desc_ui] = []
                    
                elif child.tag == 'DescriptorName':
                    for gchild in child:
                        #print gchild.text, 'gchild text', gchild.tag
                        #print 'DescriptorName found:', gchild.text
                        one_child_dict['term_set'].add(gchild.text)
                        
                            
                elif child.tag == 'TreeNumberList':
                    print len(child), 'TreeNumber Elements'
                    one_child_dict['TreeNumbers'] = []
                    for gchild in child:
                        tree_num = gchild.text
                        one_child_dict['TreeNumbers'].append(tree_num)
                        desc_tree_dict[desc_ui].append(tree_num)
                        
                    #print 'Treenumbers:', one_child_dict['TreeNumbers']
                        
                        
                        
                elif child.tag == 'ConceptList':
                
                    one_child_dict['ConceptUIs'] = []
                
                    # (for concept in concept list)
                    print 'NUMBER CONCEPTS:', len(child)
                    for gchild in child:
                        # (for information in concept)
                        for ggchild in gchild:
                        
                            if ggchild.tag == 'TermList':
                                
                                # (for term in termlist)
                                for gggchild in ggchild:
                                    #print gggchild[0].text, 'Term UI'
                                    #print gggchild[1].text, 'Term String'
                                    one_child_dict['term_set'].add(gggchild[1].text)
                                   #  print gggchild.tag, gggchild.text, 'GGGCHILD'
#                                     for ggggchild in gggchild:
#                                         print ggggchild.tag, ggggchild.text, 'GGGGCHILD'
                                print 'TERM SET', one_child_dict['term_set']
                                
                            elif ggchild.tag == 'ConceptName':
                             print 'ConceptName:', ggchild[0].text
                             one_child_dict['term_set'].add(ggchild[0].text)                            
                
                else: 
                    continue
                    
        print 'ONE CHILD DICT:', one_child_dict
        desc_dict_list.append(one_child_dict)
            
    print len(desc_dict_list)
    
    return (desc_dict_list, desc_tree_dict)


def parse_supp_file(supp_file):
    '''Parses mesh supp xml file and returns a list (supp_dict_list) of dictionaries (one_child_dict); Dictionaries have the keys 
    'SupplementalRecordUI', 'SupplementalRecordName', 'term_list', 'Mapping:ReferredDescriptors'''
    
    input_file = supp_file
    
    print 'PROCESSING SUPP FILE'

    tree = ET.parse(input_file)

    supp_dict_list = []

    root = tree.getroot()

    print len(root), 'ENTRIES IN SUPP FILE'

    tag_list = []

    for i in range(len(root)):

        print i, '-------'
        print 'Parent number', i, ':', root[i]
        one_child_dict = {}
        
        one_child_dict['term_set'] = set([])
        
        
        for child in root[i]:
                
                
                if child.tag == 'SupplementalRecordUI':
                    #print 'SupplementalRecordUI found:', child.text
                    one_child_dict['SupplementalRecordUI'] = child.text
                
                elif child.tag == 'SupplementalRecordName':
                    for gchild in child:
                        #print gchild.text, 'gchild text', gchild.tag
                        #print 'SupplementalRecordName found:', gchild.text
                        if not 'SupplementalRecordName' in one_child_dict:
                            one_child_dict['SupplementalRecordName'] = gchild.text
                        else: 
                            print 'ERROR: More than one SupplementalRecordName'
                            raise KeyError
                            
                elif child.tag == 'HeadingMappedToList':
                    #print len(child), 'Mapped Elements'
                    one_child_dict['Mapping:ReferredDescriptors'] = []
                    for gchild in child:
                        print gchild.tag, 'GCHILD TAG1'
                        for ggchild in gchild:
                            if ggchild.tag == 'DescriptorReferredTo':
                                for gggchild in ggchild:
                                    print gggchild.tag, 'GGGCHILD TAG1'
                                    if gggchild.tag == 'DescriptorUI':
                                        one_child_dict['Mapping:ReferredDescriptors'].append(gggchild.text)
                        
                  
                        
                elif child.tag == 'ConceptList':
                
                    for gchild in child:
                        # (for information in concept)
                        for ggchild in gchild:
                        
                            if ggchild.tag == 'TermList':
                                
                                # (for term in termlist)
                                for gggchild in ggchild:
                                    #print gggchild[0].text, 'Term UI'
                                    #print gggchild[1].text, 'Term String'
                                    one_child_dict['term_set'].add(gggchild[1].text)
                                    for ggggchild in gggchild:
                                        pass
                                print 'TERM SET', one_child_dict['term_set']
                                
                            elif ggchild.tag == 'ConceptName':
                             #print 'ConceptName:', ggchild[0].text
                             one_child_dict['term_set'].add(ggchild[0].text)
                             
                
                else: 
                    continue
                    
        print 'ONE CHILD DICT:', one_child_dict
        supp_dict_list.append(one_child_dict)
            
    print len(supp_dict_list)
    
    return supp_dict_list
    
    
def desc2ontogene_headers(desc_dict_list):
    '''Converts the information from the parsed xml file to a list of dictionaries containing the ontogene headers as keys.
    Ontogene header: 'oid', 'term', 'original_id', 'resource', 'entity_type', 'preferred_term' '''
    
    mesh_trees = {'A' : 'Anatomy', 'B' : 'Organisms', 'C' : 'Diseases', 'D' : 'Chemicals and Drugs', 'E' : 'Analytical,Diagnostic and Therapeutic Techniques and Equipment',
    'F' : 'Psychiatry and Psychology', 'G' : 'Phenomena and Processes', 'H' : 'Disciplines and Occupations', 'I' : 'Anthropology,Education,Sociology and Social Phenomena', 'J' : 'Technology,Industry,Agriculture',
    'K' : 'Humanities', 'L' : 'Information Science', 'M' : 'Named Groups', 'N' : 'Health Care', 'V' : 'Publication Characteristics', 
    'Z' : 'Geographicals'}
    
    ontogene_dict_list = []

    for one_dict in desc_dict_list:
    
        try:
            for tree_number in one_dict['TreeNumbers']:
        
                ontogene_dict_temp = {}
            
                branch_id = tree_number[0]
                branch_cat = mesh_trees[branch_id]
                ontogene_dict_temp['entity_type'] = branch_cat
                # standardize entity type name?
            
                term_set = one_dict['term_set']
            
                for one_term in term_set:
                    ontogene_dict_temp2 = ontogene_dict_temp.copy()
                    ontogene_dict_temp2['term'] = one_term
                    ontogene_dict_temp2['original_id'] = one_dict['DescriptorUI']
                    ontogene_dict_temp2['preferred_term'] = one_dict['DescriptorName']
                    ontogene_dict_temp2['resource'] = 'mesh_desc'
                
                
                    for tree_number in one_dict['TreeNumbers']:
                        print one_dict
                        ontogene_dict = ontogene_dict_temp2.copy()
                    
                        branch_id = tree_number[0]
                        branch_cat = mesh_trees[branch_id]
                        ontogene_dict['entity_type'] = branch_cat
                        # standardize entity type name?
                
                        # Is this the right place to insert oid? (One per term) Or better one per entity type? (several terms per oid?)
                        ontogene_dict['oid'] = ' '
        
                    
                        ontogene_dict_list.append(ontogene_dict)
        except KeyError:
            print one_dict, 'NO TREE NUMBERS'
                
    print 'DESC OG DICT LIST', ontogene_dict_list
                
    return ontogene_dict_list
    
    
    
def supp2ontogene_headers(supp_dict_list, desc_tree_dict):
    '''Converts the information from the parsed xml file to a list of dictionaries containing the ontogene headers as keys.
    Ontogene header: 'oid', 'term', 'original_id', 'resource', 'entity_type', 'preferred_term' '''
    
    mesh_trees = {'A' : 'Anatomy', 'B' : 'Organisms', 'C' : 'Diseases', 'D' : 'Chemicals and Drugs', 'E' : 'Analytical,Diagnostic and Therapeutic Techniques and Equipment',
    'F' : 'Psychiatry and Psychology', 'G' : 'Phenomena and Processes', 'H' : 'Disciplines and Occupations', 'I' : 'Anthropology,Education,Sociology and Social Phenomena', 'J' : 'Technology,Industry,Agriculture',
    'K' : 'Humanities', 'L' : 'Information Science', 'M' : 'Named Groups', 'N' : 'Health Care', 'V' : 'Publication Characteristics', 
    'Z' : 'Geographicals'}
    
    ontogene_dict_list = []

    for one_dict in supp_dict_list:
        
        ref_descs = one_dict['Mapping:ReferredDescriptors']
    
        for desc_id_raw in ref_descs:
            
        
            desc_id = desc_id_raw.lstrip('*')
            print desc_id, 'DESC ID'
            
            tree_number_list = desc_tree_dict[desc_id]
            
            for tree_number in tree_number_list:
                ontogene_dict_temp = {}
                
                branch_id = tree_number[0]
                branch_cat = mesh_trees[branch_id]
                ontogene_dict_temp['entity_type'] = branch_cat
                # standardize entity type name?
            
                term_set = one_dict['term_set']
            
                for one_term in term_set:
                    ontogene_dict_temp2 = ontogene_dict_temp.copy()
                    ontogene_dict_temp2['term'] = one_term
                    ontogene_dict_temp2['original_id'] = one_dict['SupplementalRecordUI']
                    ontogene_dict_temp2['preferred_term'] = one_dict['SupplementalRecordName']
                    ontogene_dict_temp2['resource'] = 'mesh_supp'
                    ontogene_dict_temp2['oid'] = 'none'
                    
                    ontogene_dict_list.append(ontogene_dict_temp2)
                
    print 'SUPP OG DICT LIST', ontogene_dict_list
                
    return ontogene_dict_list
    
    
    
def dict_to_file(dict_list, output_file):
    ''' takes a list of dictionaries and writes it to a csv file'''

    csv_file = codecs.open(output_file, 'wt', 'utf-8')

    fieldnames = ['original_id', 'term', 'entity_type','preferred_term', 'resource', 'oid']
    #fieldnames = ['T_LABEL','W_TEXT','W_LEMMA']

    print >> sys.stderr, '# STATUS: fieldnames', " ".join(fieldnames)


    try:

        writer = UnicodeDictWriter(csv_file, dialect= csv.excel_tab, fieldnames=fieldnames, quotechar=str("\""), quoting= csv.QUOTE_NONE, restval='__')

        headers = dict( (n,n) for n in fieldnames )


        #writer.writerow(headers)

        for one_dict in dict_list:
            writer.writerow(one_dict)

    finally: csv_file.close()






def process(options=None, args=None):
    """
    Do the processing.

    The options object should be used as an argument to almost all functions.
    This gives easy access to all global parameters.
    """
    #if options.debug:
    #    print >>sys.stderr, options

    #print sys.stdin, 'test'

    print 'OPTIONS:', options

    desc_file = args[0]

    supp_file = args[1]
    
    output_file = args[2]
    
    (desc_dict_list, desc_tree_dict) = parse_desc_file(desc_file)

    supp_dict_list = parse_supp_file(supp_file)
    
    desc_ontogene_headers = desc2ontogene_headers(desc_dict_list)
    supp_ontogene_headers = supp2ontogene_headers(supp_dict_list, desc_tree_dict)
    
    output_dict = desc_ontogene_headers + supp_ontogene_headers
    
    dict_to_file(output_dict, output_file)




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
                      help='do not print status messages to stderr')
    parser.add_option('-d', '--debug',
                      action='store_true', dest='debug', default=False,
                      help='print debug information')



    (options, args) = parser.parse_args()

    if options.debug: print >> sys.stderr, '# Starting processing'

    process(options=options,args=args)




    sys.exit(0) # Everything went ok!

if __name__ == '__main__':
    cProfile.run('main()', 'mesh.profile1')
    
    #main()

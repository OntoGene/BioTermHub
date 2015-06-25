#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Python Program Description: Script for ....

"""
from optparse import OptionParser
import codecs
import sys
import os
import shutil
import random
import nltk
import collections
import csv
import cStringIO
import xml.etree.cElementTree as ET
import oid_generator


import cProfile

sys.path.append('../lib')
#sys.path.append('/Users/tilia/Projects/cgtt_combined_terminologies/terminology_tool/lib')

from unicode_csv import UnicodeDictWriter





def parse_desc_file(desc_file, options=None, args=None):
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
                        one_child_dict['DescriptorName'] = gchild.text
                        
                            
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


def parse_supp_file(supp_file, options=None, args=None):
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
    
    
def desc2ontogene_headers(relevant_trees, desc_dict_list, options=None, args=None):
    '''Converts the information from the parsed xml file to a list of dictionaries containing the ontogene headers as keys.
    Ontogene header: 'oid', 'term', 'original_id', 'resource', 'entity_type', 'preferred_term' '''
    
    mesh_trees = {'A' : 'Anatomy', 'B' : 'Organisms', 'C' : 'Diseases', 'D' : 'Chemicals and Drugs', 'E' : 'Analytical,Diagnostic and Therapeutic Techniques and Equipment',
    'F' : 'Psychiatry and Psychology', 'G' : 'Phenomena and Processes', 'H' : 'Disciplines and Occupations', 'I' : 'Anthropology,Education,Sociology and Social Phenomena', 'J' : 'Technology,Industry,Agriculture',
    'K' : 'Humanities', 'L' : 'Information Science', 'M' : 'Named Groups', 'N' : 'Health Care', 'V' : 'Publication Characteristics', 
    'Z' : 'Geographicals'}
    
    new_mesh_dict = {'Diseases' : 'disease', 'Chemicals and Drugs' : 'chemical', 'Organisms' : 'organism'}
    
    ontogene_dict_list = []

    for one_dict in desc_dict_list:
    
        try:
    
            branch_set = set([one_tree_number[0] for one_tree_number in one_dict['TreeNumbers']])
            
            
            if branch_set.intersection(relevant_trees):
    
                for entity_type_code in branch_set:
                    # adding information for different entity types
        
                    ontogene_dict_temp = {}
                
                     # Is this the right place to insert oid? (One per mesh id per entity type)
                    ontogene_dict_temp['oid'] = oid_generator.OID.get()
                    # ADD OID
            
                    branch_cat = mesh_trees[entity_type_code]
                    try:
                        ontogene_dict_temp['entity_type'] = new_mesh_dict[branch_cat]
                    except KeyError: 
                        ontogene_dict_temp['entity_type'] = branch_cat
                    # standardize entity type name?
            
                    term_set = one_dict['term_set']
            
                    for one_term in term_set:
                        # adding all synonyms for all entity types/tree numbers
                        ontogene_dict_temp2 = ontogene_dict_temp.copy()
                        ontogene_dict_temp2['term'] = one_term
                        ontogene_dict_temp2['original_id'] = one_dict['DescriptorUI']
                        ontogene_dict_temp2['preferred_term'] = one_dict['DescriptorName']
                        ontogene_dict_temp2['resource'] = 'mesh_desc'
                
                        ontogene_dict_list.append(ontogene_dict_temp2)
                    
        except KeyError:
            print one_dict, 'NO TREE NUMBERS'
                
    print 'DESC OG DICT LIST', ontogene_dict_list
                
    return ontogene_dict_list
    
    
    
def supp2ontogene_headers(relevant_trees, supp_dict_list, desc_tree_dict, options=None, args=None):
    '''Converts the information from the parsed xml file to a list of dictionaries containing the ontogene headers as keys.
    Ontogene header: 'oid', 'term', 'original_id', 'resource', 'entity_type', 'preferred_term' '''
    
    mesh_trees = {'A' : 'Anatomy', 'B' : 'Organisms', 'C' : 'Diseases', 'D' : 'Chemicals and Drugs', 'E' : 'Analytical,Diagnostic and Therapeutic Techniques and Equipment',
    'F' : 'Psychiatry and Psychology', 'G' : 'Phenomena and Processes', 'H' : 'Disciplines and Occupations', 'I' : 'Anthropology,Education,Sociology and Social Phenomena', 'J' : 'Technology,Industry,Agriculture',
    'K' : 'Humanities', 'L' : 'Information Science', 'M' : 'Named Groups', 'N' : 'Health Care', 'V' : 'Publication Characteristics', 
    'Z' : 'Geographicals', 'empty_branch' : '-'}
    
    new_mesh_dict = {'Diseases' : 'disease', 'Chemicals and Drugs' : 'chemical', 'Organisms' : 'organism'}
    
    ontogene_dict_list = []

    for one_dict in supp_dict_list:
        
        ref_descs = one_dict['Mapping:ReferredDescriptors']
    
        for desc_id_raw in ref_descs:
            
        
            desc_id = desc_id_raw.lstrip('*')
            #print desc_id, 'DESC ID'
            
            try:
                tree_number_list = desc_tree_dict[desc_id]
                branch_set = set([one_tree_number[0] for one_tree_number in tree_number_list])
                
            except KeyError:
                print 'NO TREE INFORMATION FOUND FOR DESCRIPTOR ID', desc_id
                branch_set = set(['empty_branch'])
                
                
            if branch_set.intersection(relevant_trees):
            
                for entity_type_code in branch_set:
                    ontogene_dict_temp = {}
                
                    # Is this the right place to insert oid? (One per mesh id per entity type)
                    ontogene_dict_temp['oid'] = oid_generator.OID.get()
                    # ADD OID
                
                    branch_cat = mesh_trees[entity_type_code]
                    try:
                        ontogene_dict_temp['entity_type'] = new_mesh_dict[branch_cat]
                    except KeyError: 
                        ontogene_dict_temp['entity_type'] = branch_cat
                        # standardize entity type name?
            
                    term_set = one_dict['term_set']
            
                    for one_term in term_set:
                        ontogene_dict_temp2 = ontogene_dict_temp.copy()
                        ontogene_dict_temp2['term'] = one_term
                        ontogene_dict_temp2['original_id'] = one_dict['SupplementalRecordUI']
                        ontogene_dict_temp2['preferred_term'] = one_dict['SupplementalRecordName']
                        ontogene_dict_temp2['resource'] = 'mesh_supp'
                    
                        ontogene_dict_list.append(ontogene_dict_temp2)
                
    print 'SUPP OG DICT LIST', ontogene_dict_list
                
    return ontogene_dict_list
    
    
    
def dict_to_file(dict_list, output_file, options=None, args=None):
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






def process(relevant_trees, options=None, args=None):
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
    
    (desc_dict_list, desc_tree_dict) = parse_desc_file(desc_file, options=options, args=args)

    supp_dict_list = parse_supp_file(supp_file, options=options, args=args)
    
    desc_ontogene_headers = desc2ontogene_headers(relevant_trees, desc_dict_list, options=options, args=args)
    supp_ontogene_headers = supp2ontogene_headers(relevant_trees, supp_dict_list, desc_tree_dict, options=options, args=args)
    
    output_dict = desc_ontogene_headers + supp_ontogene_headers
    
    dict_to_file(output_dict, output_file, options=options, args=args)




def main():
    """
    Invoke this module as a script
    """
    usage = "usage: %prog [options] input: Mesh desc file (args[0]); Mesh supp file (args[1]). output: Ouput file (args[2])"
    parser = OptionParser(version='%prog 0.99', usage=usage)

    parser.add_option('-l', '--logfile', dest='logfilename',
                      help='write log to FILE', metavar='FILE')
    parser.add_option('-q', '--quiet',
                      action='store_true', dest='quiet', default=False,
                      help='do not print status messages to stderr')
    parser.add_option('-d', '--debug',
                      action='store_true', dest='debug', default=False,
                      help='print debug information')
    parser.add_option('-e', '--entity_trees',
                      dest='entity_trees',default=False,
                      help='give lists of first letters of tree ids to specify entity types; https://www.nlm.nih.gov/mesh/trees.html')
                      



    (options, args) = parser.parse_args()

    if options.debug: print >> sys.stderr, '# Starting processing'
    
    
    mesh_trees = {'A' : 'Anatomy', 'B' : 'Organisms', 'C' : 'Diseases', 'D' : 'Chemicals and Drugs', 'E' : 'Analytical,Diagnostic and Therapeutic Techniques and Equipment',
    'F' : 'Psychiatry and Psychology', 'G' : 'Phenomena and Processes', 'H' : 'Disciplines and Occupations', 'I' : 'Anthropology,Education,Sociology and Social Phenomena', 'J' : 'Technology,Industry,Agriculture',
    'K' : 'Humanities', 'L' : 'Information Science', 'M' : 'Named Groups', 'N' : 'Health Care', 'V' : 'Publication Characteristics', 
    'Z' : 'Geographicals', 'empty_branch' : 'missing'}
    
    if options.entity_trees:
        tree_list = options.entity_trees.split(',')
        tree_list.append('empty_branch')
        print tree_list, 'TREE LIST'
        #relevant_trees = set(tree_list)
        relevant_trees = set(tree_list)
    else: relevant_trees = set(mesh_trees.keys())
    
    print relevant_trees, 'REL TREES'

    process(relevant_trees, options=options,args=args)




    sys.exit(0) # Everything went ok!

if __name__ == '__main__':
    # Prevent Encoding exceptions in Python 2.x
    # Edit by Lenz: I moved those here because otherwise I get a nasty
    # double-encoding when importing this script as a module in ipython
    # (an interactive Python interpreter).
    sys.stdout = codecs.getwriter('utf-8')(sys.__stdout__)
    sys.stderr = codecs.getwriter('utf-8')(sys.__stderr__)
    sys.stdin = codecs.getreader('utf-8')(sys.__stdin__)

    cProfile.run('main()', 'mesh.profile1')
    
    #main()

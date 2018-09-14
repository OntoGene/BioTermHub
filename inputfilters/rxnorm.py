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
import itertools as it

from termhub.inputfilters._base import IterConceptRecordSet


class RecordSet(IterConceptRecordSet):
    '''
    Record collector for RxNorm RFF.
    '''

    resource = 'RX Norm'
    entity_type = 'clinical_drug' # could be 'drug'?

    dump_fn = 'RXNCONSO.RFF'
    
    # test this in June again to see if this gives the new file, too?
    import os, ssl
    # if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
    #     getattr(ssl, '_create_unverified_context', None)): 
    #     ssl._create_default_https_context = ssl._create_unverified_context
    
    remote = 'http://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_current.zip'
    source_ref = 'http://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html'

    @classmethod
    def _update_steps(cls):
        return ('zip', [('rrf/RXNCONSO.RRF',)], cls.preprocess)
    
    @classmethod
    def preprocess(self, stream):
        
        zip_to_text = io.TextIOWrapper(stream[0])
        reader = csv.reader(zip_to_text, delimiter="|")
        
        # general file format as described here:
        # https://www.nlm.nih.gov/research/umls/rxnorm/docs/2018/rxnorm_doco_full_2018-1.html#s12_4
        # row[0]: ID, row[11]: source, row[12]: term type, row[14]: term string
        # reader = csv.reader(stream,delimiter='|')
        
        # we keep the last read line in memory, 
        # as well as list
        # of all term variations for the same ID
        last_row = next(reader)
        current_id = last_row[0]
        terms = [ last_row[14] ]
        
        for row in reader:
            
            # this should produce that output:
            # '{}\t{}\t{}\t{}\n'.format(id_, rank, pref, '\t'.join(terms))
            
            # if ID is the same, then we just
            # add terms to the list
            if current_id == row[0]:
                terms.append(row[14])
            
            # otherwise we write line
            # and update / reset counters etc.   
            else:
                
                line = '{}\t{}\t{}\n'.format(row[0], self.prefered_term(terms), '\t'.join(terms))
                
                current_id = row[0]
                last_row = row
                terms = [ row[14] ]
                                
                yield line.encode('utf-8')
        
        # write the last line
        terms_string = '(' + ', '.join(terms) + ')'
        line = '\t'.join([last_row[0],self.prefered_term(terms),terms_string,last_row[12],last_row[11]]) + '\n'
        yield line.encode('utf-8')

    @staticmethod
    # obviously, this can use alot more work        
    def prefered_term(terms):
        '''
        Given list of term variations, finds what is most likely to be the preferred term
        * groups all variations by their lowercased version, then takes the most frequent group
        * if there's a match, takes the longest string
        '''        
        
        lower = [ term.lower() for term in terms ]
        
        # if they're all the same, there's nothing for us to do
        if len(set(lower)) == 1:
            return lower[0]
        
        # let's see if one get's the most counts
        counts = { term : lower.count(term) for term in set(lower) }
        max_count = max(counts.values())
        frequentest_terms = [ term for term, count in counts.items() if count == max_count ]
        if len(frequentest_terms) == 1:
            return frequentest_terms[0]
        
        # take the longest one from those that have the most counts
        lengths = { term : len(term) for term in frequentest_terms }
        max_length = max(lengths.values())
        longest_terms = [ term for term, length in lengths.items() if length == max_length ]
        
        # if they have equal length, also, then we just take the first one
        return longest_terms[0] 

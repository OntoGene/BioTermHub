#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016
#         Nico Colic, 2018


'''
Parse RxNorm Current Prescribable Content ("RXNCONSO.RRF").
'''

# https://www.nlm.nih.gov/research/umls/rxnorm/docs/2018/rxnorm_doco_full_2018-1.html#s12_4

# RXNSAT.RRF :
# RXNREL.RRF : atoms and concept links, links RXAUI and RXCUI


import io
import csv

from termhub.inputfilters._base import IterConceptRecordSet


class RecordSet(IterConceptRecordSet):
    '''
    Record collector for RxNorm RRF.
    '''

    resource = 'RxNorm'
    entity_type = 'clinical_drug'  # or just 'drug'? 'chemical'?

    dump_fn = 'RXNCONSO.RRF'

    remote = 'http://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_current.zip'
    source_ref = 'http://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html'

    @classmethod
    def _update_steps(cls):
        return ('zip', [('rrf/RXNCONSO.RRF',)], cls.preprocess)

    @classmethod
    def preprocess(cls, stream):
        '''
        Parse RRF and produce lines in the canonical _iter_concepts format.
        '''
        zip_to_text = io.TextIOWrapper(stream[0], encoding='utf-8')
        reader = csv.reader(zip_to_text, delimiter="|")
        for id_, terms in cls._prep_concepts(reader):
            pref = cls.preferred_term(terms)
            line = '{}\t{}\t{}\n'.format(id_, pref, '\t'.join(terms))
            yield line.encode('utf-8')

    @staticmethod
    def _prep_concepts(rows):
        '''
        Aggregate subsequent lines from the same concept.

        Relevant columns:
            0:  ID
            14: term string
        '''
        id_ = None
        terms = []
        for row in rows:
            if row[0] != id_:
                if terms:
                    yield id_, terms
                id_ = row[0]
                terms.clear()
            terms.append(row[14])
        # Don't forget the last concept.
        if terms:
            yield id_, terms

    @staticmethod
    # obviously, this can use alot more work
    def preferred_term(terms):
        '''
        Given list of term variations, finds what is most likely to be the preferred term
        * groups all variations by their lowercased version, then takes the most frequent group
        * if there's a match, takes the longest string
        '''

        lower = [term.lower() for term in terms]

        # if they're all the same, there's nothing for us to do
        if len(set(lower)) == 1:
            return lower[0]

        # let's see if one get's the most counts
        counts = {term: lower.count(term) for term in set(lower)}
        max_count = max(counts.values())
        frequentest_terms = [term for term, count in counts.items() if count == max_count]
        if len(frequentest_terms) == 1:
            return frequentest_terms[0]

        # take the longest one from those that have the most counts
        lengths = {term: len(term) for term in frequentest_terms}
        max_length = max(lengths.values())
        longest_terms = [term for term, length in lengths.items() if length == max_length]

        # if they have equal length, also, then we just take the first one
        return longest_terms[0]

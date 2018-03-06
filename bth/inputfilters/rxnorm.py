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


import csv
import codecs
from collections import Counter

from ._base import IterConceptRecordSet, UMLSIterConceptMixin


class RecordSet(UMLSIterConceptMixin, IterConceptRecordSet):
    '''
    Record collector for RxNorm RRF.
    '''

    resource = 'RxNorm'
    entity_type = 'clinical_drug'  # or just 'drug'? 'chemical'?

    dump_fn = 'rxnorm.tsv'

    remote = 'http://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_current.zip'
    source_ref = 'http://www.nlm.nih.gov/research/umls/rxnorm/'
    umls_abb = 'RXNORM'

    @classmethod
    def _update_steps(cls):
        return ('zip', 'rrf/RXNCONSO.RRF', cls.preprocess)

    @classmethod
    def preprocess(cls, stream):
        '''
        Parse RRF and produce lines in the canonical _iter_concepts format.
        '''
        zip_to_text = codecs.iterdecode(stream, encoding='utf-8')
        reader = csv.reader(zip_to_text, delimiter="|")
        cui_map = cls._load_cui_map()
        for id_, terms in cls._prep_concepts(reader):
            pref = cls.preferred_term(terms)
            for cui, t in cls._assign_cuis(id_, set(terms), cui_map):
                yield cls._canonical_line(id=id_, cui=cui, pref=pref, terms=t)

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
    def preferred_term(terms):
        '''
        Heuristically determine the best candidate.

        Take the most frequent, counting case insensitively.
        Break ties first by preferring longer names, then by
        preferring title-cased names.
        '''
        frequencies = Counter(term.lower() for term in terms)
        def _sortkey(term):
            titlecase = sum(tok.istitle() for tok in term.split())
            return frequencies[term.lower()], len(term), titlecase

        return max(terms, key=_sortkey)

#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect ChEBI chemicals ("chebi.obo").
'''


import re

from termhub.inputfilters._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for ChEBI.
    '''

    ambig_unit = "terms"
    resource = 'ChEBI'
    entity_type = 'chemical'
    dump_fn = 'chebi.obo'

    @classmethod
    def _relevant_synonym(cls, syntype):
        '''
        Exclude formulas and InChiKeys.
        '''
        return not cls.exclude.search(syntype)

    exclude = re.compile(r'FORMULA|InChI|SMILE')

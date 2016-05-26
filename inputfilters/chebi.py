#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect CHEBI chemicals ("chebi.obo"), parsed with Orange.bio.
'''


from Orange.bio.ontology import OBOParser

from termhub.inputfilters.abc import AbstractRecordSet
from termhub.lib.tools import Fields


DUMP_FN = 'chebi.obo'


class RecordSet(AbstractRecordSet):
    '''
    Record collector for CHEBI through OBO.
    '''

    ambig_unit = "terms"
    resource = 'CHEBI'
    entity_type = 'chemical'

    def __init__(self, fn=DUMP_FN, **kwargs):
        super().__init__(fn, **kwargs)

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for stanza in self._iter_stanzas():
            pass

    def _iter_stanzas(self):
        '''
        Collect all information for a term.
        '''
        with open(self.fn) as f:
            for event, item in OBOParser(file):
                if event == 'START_STANZA':
                    term = {}
                    term['synonyms'] = []
                elif event == 'TAG_VALUE':
                    tag, value = item
                    if tag == 'name':
                        term['term'] = value
                    elif tag == 'id':
                        term['id'] = value
                    elif tag == 'synonym':
                        synonym = value.split('"')[1]
                        if synonym != '.':
                            term['synonym_list'].append(synonym)
                elif event == 'CLOSE_STANZA':
                    yield term


#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect ChEBI chemicals ("chebi.obo").
'''


import re

from termhub.inputfilters.recordset import AbstractRecordSet
from termhub.lib.tools import Fields


DUMP_FN = 'chebi.obo'


class RecordSet(AbstractRecordSet):
    '''
    Record collector for ChEBI.
    '''

    ambig_unit = "terms"
    resource = 'ChEBI'
    entity_type = 'chemical'

    def __init__(self, fn=DUMP_FN, **kwargs):
        super().__init__(fn, **kwargs)

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for concept in self._iter_stanzas():
            oid = next(self.oidgen)

            terms = set(s for s, t in concept['synonyms']
                          if self._relevant_synonym(t))
            terms.add(concept['pref'])

            if self.collect_stats:
                self.update_stats(len(terms))

            for term in terms:
                entry = Fields(oid,
                               self.resource,
                               concept['id'],
                               term,
                               concept['pref'],
                               self.entity_type)
                yield entry


    def _iter_stanzas(self):
        '''
        Parse the .obo stanzas.
        '''
        tag_value = re.compile(r'(\w+): (.+)')
        synonym_type = re.compile(r'"(.*)" (.+)')

        with open(self.fn) as f:
            inside = False
            concept = {}
            for line in f:
                line = line.strip()
                if not line:
                    # Stanza has ended.
                    if 'id' in concept:
                        yield concept
                    inside = False
                    concept.clear()
                elif line == '[Term]':
                    # Stanza starts.
                    inside = True
                    concept['synonyms'] = []
                elif inside:
                    tag, value = tag_value.match(line).groups()
                    if tag == 'id':
                        concept['id'] = value
                    elif tag == 'name':
                        concept['pref'] = value
                    elif tag == 'synonym':
                        synonym, syntype = synonym_type.match(value).groups()
                        concept['synonyms'].append((synonym, syntype))
            if 'id' in concept:
                # After the final stanza: last yield.
                yield concept

    @classmethod
    def _relevant_synonym(cls, syntype):
        '''
        Exclude formulas and InChiKeys.
        '''
        return not cls.exclude.search(syntype)

    exclude = re.compile(r'FORMULA|InChI|SMILE')

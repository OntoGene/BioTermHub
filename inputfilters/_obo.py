#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parser for OBO files.
'''


import re

from termhub.inputfilters._base import AbstractRecordSet
from termhub.lib.tools import Fields


class OboRecordSet(AbstractRecordSet):
    '''
    Abstract record collector for OBO dumps.
    '''
    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for concept in self._iter_stanzas():
            oid = next(self.oidgen)

            terms = concept['synonyms']
            terms.add(concept['pref'])

            if self.collect_stats:
                self.update_stats(len(terms))

            entity_type = self._get_entity_type(concept)

            for term in terms:
                entry = Fields(oid,
                               self.resource,
                               concept['id'],
                               term,
                               concept['pref'],
                               entity_type)
                yield entry

    def _iter_stanzas(self):
        '''
        Parse the .obo stanzas.

        Do not call list(...) on this method:
        The same object is yielded in every iteration
        (with modified content).
        '''
        tag_value = re.compile(r'(\w+): (.+)')
        synonym_type = re.compile(r'"((?:[^"]|\\")*)" (.+)')

        with open(self.fn, encoding='utf-8') as f:
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
                    concept['synonyms'] = set()
                elif line == 'is_obsolete: true':
                    continue
                elif inside:
                    tag, value = tag_value.match(line).groups()
                    if tag == 'id':
                        concept['id'] = value
                    elif tag == 'namespace':
                        concept['entity_type'] = value
                    elif tag == 'name':
                        concept['pref'] = value
                    elif tag == 'synonym':
                        synonym, syntype = synonym_type.match(value).groups()
                        if self._relevant_synonym(syntype):
                            # Unescape quotes.
                            synonym = synonym.replace('\\"', '"')
                            concept['synonyms'].add(synonym)
            if 'id' in concept:
                # After the final stanza: last yield.
                yield concept

    @classmethod
    def _relevant_synonym(cls, syntype):
        '''
        Subclass hook for filtering by synonym type.
        '''
        return True

    def _get_entity_type(self, concept):
        '''
        Subclass hook for configuring the entity type.
        '''
        return self.entity_type

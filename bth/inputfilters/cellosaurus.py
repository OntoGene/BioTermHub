#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Cellosaurus entries ("cellosaurus.txt").
'''


import re

from ._base import IterConceptRecordSet


class RecordSet(IterConceptRecordSet):
    '''
    Record collector for Cellosaurus.
    '''

    resource = 'Cellosaurus'
    entity_type = 'cell_line'
    uri_prefix = 'http://web.expasy.org/cellosaurus/'

    dump_fn = 'cellosaurus.txt'
    remote = 'ftp://ftp.expasy.org/databases/cellosaurus/cellosaurus.txt'
    source_ref = 'http://web.expasy.org/cellosaurus/'

    def _iter_concepts(self):
        '''
        Extract the relevant information from Cellosaurus.
        '''
        synonym_sep = re.compile(r'\s*;\s*')

        for stanza in self._iter_stanzas():
            id_ = stanza['AC']   # accession number
            pref = stanza['ID']  # unstable identifier
            terms = set((pref,))
            if 'SY' in stanza:   # synonyms
                terms.update(synonym_sep.split(stanza['SY']))
            yield id_, pref, terms, self.entity_type, self.resource

    def _iter_stanzas(self):
        '''
        Parse the cellosaurus stanzas.

        Do not call list(...) on this method:
        The same object is yielded in every iteration
        (with modified content).
        '''
        code_content = re.compile(r'([A-Z]{2}) {3}(.+)')

        with open(self.fn, encoding='utf-8') as f:
            inside = False
            stanza = {}
            for line in f:
                if not inside:
                    if line.startswith('ID   '):
                        # Stanza start marker (also first content line).
                        inside = True
                        stanza.clear()
                if inside:
                    try:
                        code, content = code_content.match(line).groups()
                    except AttributeError:
                        # Stanza end marker.
                        inside = False
                        yield stanza
                    else:
                        stanza[code] = content

#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Cellosaurus entries ("cellosaurus.txt").
'''


import re

from termhub.inputfilters._base import AbstractRecordSet
from termhub.lib.tools import Fields


class RecordSet(AbstractRecordSet):
    '''
    Record collector for Cellosaurus.
    '''

    ambig_unit = "terms"
    resource = 'Cellosaurus'
    entity_type = 'cell_line'

    dump_fn = 'cellosaurus.txt'
    remote = 'ftp://ftp.expasy.org/databases/cellosaurus/cellosaurus.txt'

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for id_, pref, synonyms in self._iter_concepts():
            oid = next(self.oidgen)

            terms = set(synonyms)
            terms.add(pref)

            if self.collect_stats:
                self.update_stats(len(terms))

            for term in terms:
                entry = Fields(oid,
                               self.resource,
                               id_,
                               term,
                               pref,
                               self.entity_type)
                yield entry

    def _iter_concepts(self):
        '''
        Extract the relevant information from Cellosaurus.
        '''
        synonym_sep = re.compile(r'\s*;\s*')

        for stanza in self._iter_stanzas():
            synonyms = stanza.get('SY', [])
            if synonyms:
                synonyms = synonym_sep.split(synonyms)
            yield stanza['AC'], stanza['ID'], synonyms

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

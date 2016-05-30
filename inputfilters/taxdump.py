#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's Taxonomy dump ("names.dmp.trunc").
'''


import re
from collections import defaultdict

from termhub.inputfilters.recordset import AbstractRecordSet
from termhub.lib.tools import Fields


DUMP_FN = 'names.dmp.trunc'


class RecordSet(AbstractRecordSet):
    '''
    Record collector for NCBI Taxonomy dumps.
    '''

    ambig_unit = "terms"
    resource = 'NCBI Taxonomy'
    entity_type = 'species'

    def __init__(self, fn=DUMP_FN, **kwargs):
        super().__init__(fn, **kwargs)

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for concept in self._iter_stanzas():
            oid = next(self.oidgen)

            terms = self._extract_terms(concept)
            pref = self._get_preferred(concept)

            if self.collect_stats:
                self.update_stats(len(terms))

            for term in terms:
                entry = Fields(oid,
                               self.resource,
                               concept['id'],
                               term,
                               pref,
                               self.entity_type)
                yield entry

    def _iter_stanzas(self):
        '''
        Collect adjacent lines with the same ID.
        '''
        with open(self.fn) as f:
            concept = defaultdict(list)
            previous_id = None
            for line in f:
                line = line.rstrip("\t|\r\n").split("\t|\t")
                id_, name, unique_name, name_class = line
                if id_ != previous_id:
                    if concept:
                        yield dict(concept)
                        concept.clear()
                    concept['id'] = id_
                    previous_id = id_
                concept[name_class].append((name, unique_name))
        if concept:
            yield dict(concept)

    @classmethod
    def _extract_terms(cls, concept):
        '''
        Select and edit synonym entries.
        '''
        terms = set()
        for name_class, entries in concept.items():
            if name_class in ('id', 'authority'):
                continue
            for name, unique_name in entries:
                if name.startswith('no culture available'):
                    continue
                if name_class == 'synonym':
                    name = cls._strip_citations(name)
                terms.add(name)
                if unique_name:
                    terms.add(unique_name)
        return terms

    @classmethod
    def _strip_citations(cls, name):
        '''
        Remove author-year references from species names.
        '''
        # If there is a quoted portion, just take that
        # (there shouldn't be any citation inside quotes).
        match = cls.p_quoted.match(name)
        if match:
            return match.group(1)

        # Remove any parenthesized text and whatever follows.
        match = cls.p_paren_follows.match(name)
        if match:
            name = match.group(1)

        # Remove Author-Year citations:
        # - Author1 YYYY
        # - Author1 and Author2 YYYY
        # - Author1 et al. YYYY
        name = cls.p_citation.sub('', name)

        return name

    p_quoted = re.compile(r'"(.+)".*')
    p_paren_follows = re.compile(r'(.+?) \(')
    p_citation = re.compile(r'\S*( and \S*| et al\.)? \d{4}')

    @staticmethod
    def _get_preferred(concept):
        '''
        Use the scientific name as the preferred term.

        It seems to be always present, and there is only one per concept.
        '''
        return concept['scientific name'][0][0]  # first occ, first pair member

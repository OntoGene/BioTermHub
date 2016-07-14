#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's Taxonomy dump ("names.dmp" and "nodes.dmp").
'''


import re
from collections import defaultdict

from termhub.inputfilters._base import AbstractRecordSet
from termhub.lib.tools import Fields


class RecordSet(AbstractRecordSet):
    '''
    Record collector for NCBI Taxonomy dumps.
    '''

    ambig_unit = "terms"
    resource = 'NCBI Taxonomy'
    entity_type = 'organism'

    dump_fn = ('names.dmp', 'nodes.dmp')
    remote = 'ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz'

    def __init__(self, ranks='species', **kwargs):
        '''
        Args:
            ranks (str or sequence): include only these ranks
                (taxa, eg. "kingdom", "superorder" etc.).
                The special name "all" accepts all ranks.
        '''
        super().__init__(**kwargs)
        self.valid_ranks = self._parse_rank_spec(ranks)

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for concept in self._iter_stanzas():
            if concept['rank'] not in self.valid_ranks:
                continue

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
        with open(self.fn[0], encoding='utf-8') as names, \
                open(self.fn[1], encoding='utf-8') as nodes:
            concept = defaultdict(list)
            node_id = None
            previous_id = None
            for line in names:
                line = line.rstrip("\t|\r\n").split("\t|\t")
                id_, name, unique_name, name_class = line
                while node_id != id_:
                    node_id, rank = next(nodes).split('\t|\t', 3)[::2]
                if id_ != previous_id:
                    if concept:
                        yield dict(concept)
                        concept.clear()
                    concept['id'] = id_
                    concept['rank'] = rank
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
            if name_class in ('id', 'rank', 'authority'):
                continue
            for name, unique_name in entries:
                if name.startswith('no culture available'):
                    continue
                if name_class == 'synonym':
                    if name in ('all', 'root'):
                        # Skip these top-level meta-entries.
                        continue
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

    @staticmethod
    def _parse_rank_spec(ranks):
        if ranks == 'all':
            return Universe()  # "contains" everything
        if isinstance(ranks, str):
            ranks = (ranks,)
        return frozenset(ranks)

    @classmethod
    def update_info(cls):
        # `fn` is needed twice: first as extraction target,
        # second as output file name.
        return [(cls.remote, 'tar', [(fn, fn) for fn in cls.dump_fn])]


class Universe(object):
    'A dummy object that claims to contain everything.'
    def __contains__(self, _):
        return True

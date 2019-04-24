#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's Taxonomy dump ("names.dmp" and "nodes.dmp" -> "ncbitax.tsv").
'''


import re
import codecs
from collections import defaultdict

from ._base import IterConceptRecordSet, UMLSIterConceptMixin


class RecordSet(UMLSIterConceptMixin, IterConceptRecordSet):
    '''
    Record collector for NCBI Taxonomy dumps.
    '''

    resource = 'NCBI Taxonomy'
    entity_type = 'organism'
    uri_prefix = 'http://www.ncbi.nlm.nih.gov/taxonomy/'

    dump_fn = 'ncbitax.tsv'
    targets = ('names.dmp', 'nodes.dmp')  # archive members
    remote = 'ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz'
    source_ref = 'https://www.ncbi.nlm.nih.gov/taxonomy'
    umls_abb = 'NCBI'

    def __init__(self, ranks='species', **kwargs):
        '''
        Args:
            ranks (str or sequence): include only these ranks
                (taxa, eg. "kingdom", "superorder" etc.).
                The special name "all" accepts all ranks.
        '''
        super().__init__(**kwargs)
        self.valid_ranks = self._parse_rank_spec(ranks)

    def _cui_concepts(self):
        for id_, cui, rank, pref, *terms in self._concept_rows():
            if rank in self.valid_ranks:
                yield id_, cui, pref, terms, self.entity_type, self.resource

    _line_template = '{id}\t{cui}\t{rank}\t{pref}\t{terms}\n'

    @classmethod
    def preprocess(cls, streams):
        '''
        Join names.dmp and nodes.dmp and extract ID, terms, rank.
        '''
        streams = [codecs.getreader('utf8')(s) for s in streams]
        cui_map = cls._load_cui_map()
        for concept in cls._iter_stanzas(*streams):
            pref = cls._get_preferred(concept)
            terms = cls._extract_terms(concept)
            for cui, terms in cls._assign_cuis(concept['id'], terms, cui_map):
                yield cls._canonical_line(cui=cui, pref=pref, terms=terms,
                                          **concept)

    @staticmethod
    def _iter_stanzas(names, nodes):
        '''
        Collect adjacent lines with the same ID.
        '''
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
    def _update_steps(cls):
        # The "tar" step requires a forking: a list of branches.
        # Each branch is a steps sequence, where the first element is the name
        # of the targeted archive member.
        # Here, the steps sequence is really just the member name.
        branches = [(t,) for t in cls.targets]
        return ('tar', branches, cls.preprocess)


class Universe(object):
    'A dummy object that claims to contain everything.'
    def __contains__(self, _):
        return True

#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's EntrezGene dump ("gene_info.trunc").
'''


import io

from termhub.inputfilters._base import AbstractRecordSet
from termhub.lib.tools import Fields


class RecordSet(AbstractRecordSet):
    '''
    Record collector for EntrezGene dumps.
    '''

    resource = 'EntrezGene'
    entity_type = 'gene/protein'

    dump_fn = 'gene_info.trunc'
    remote = 'ftp://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz'
    source_ref = 'https://www.ncbi.nlm.nih.gov/gene/'

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for id_, pref, terms in self._iter_concepts():
            oid = next(self.oidgen)

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
        Parse the truncated TSV.
        '''
        with open(self.fn, encoding='utf-8') as f:
            for line in f:
                id_, pref, *terms = line.rstrip('\n').split('\t')
                yield id_, pref, terms

    @classmethod
    def update_info(cls):
        return [(cls.remote, 'gz', cls.preprocess, cls.dump_fn)]

    @staticmethod
    def preprocess(stream):
        '''
        Save some space by removing unused data right away.
        '''
        lines = io.TextIOWrapper(stream, encoding='utf-8')
        next(lines)  # Throw away the header line.
        for line in lines:
            _, id_, symbol, _, synonyms, _ = line.split('\t', 5)
            if symbol != 'NEWENTRY':  # placeholders for future addition?
                terms = set((symbol,))
                if synonyms != '-':
                    terms.update(synonyms.split('|'))
                line = '{}\t{}\t{}\n'.format(id_, symbol, '\t'.join(terms))
                yield line.encode('utf-8')

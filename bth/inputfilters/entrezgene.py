#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's EntrezGene dump ("gene_info.trunc").
'''


import io

from ._base import IterConceptRecordSet


class RecordSet(IterConceptRecordSet):
    '''
    Record collector for EntrezGene dumps.
    '''

    resource = 'EntrezGene'
    entity_type = 'gene/protein'

    dump_fn = 'gene_info.trunc'
    remote = 'ftp://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz'
    source_ref = 'https://www.ncbi.nlm.nih.gov/gene/'

    @classmethod
    def _update_steps(cls):
        return ('gz', cls.preprocess)

    @classmethod
    def preprocess(cls, stream):
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
                yield cls._canonical_line(id=id_, pref=symbol, terms=terms)

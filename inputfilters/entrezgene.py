#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's EntrezGene dump ("gene_info.trunc").
'''


import io

from termhub.inputfilters._base import IterConceptRecordSet


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

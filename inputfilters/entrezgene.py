#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's EntrezGene dump ("gene_info.trunc").
'''


import io
import csv

from termhub.inputfilters._base import AbstractRecordSet
from termhub.lib.tools import Fields


class RecordSet(AbstractRecordSet):
    '''
    Record collector for EntrezGene dumps.
    '''

    ambig_unit = "terms"
    resource = 'EntrezGene'
    entity_type = 'gene/protein'

    dump_fn = 'gene_info.trunc'
    remote = 'ftp://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz'

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
        Parse the truncated TSV.
        '''
        with open(self.fn, encoding='utf-8', newline='') as f:
            reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            for id_, symbol, synonyms in reader:
                if synonyms == '-':
                    synonyms = []
                else:
                    synonyms = synonyms.split('|')
                yield id_, symbol, synonyms

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
            fields = line.split('\t', 5)
            if fields[2] != 'NEWENTRY':  # placeholders for future addition?
                line = '\t'.join((fields[1], fields[2], fields[4])) + '\n'
                yield line.encode('utf-8')

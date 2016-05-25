#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's EntrezGene dump ("gene_info.trunc").
'''


import csv

from base36gen import Base36Generator
from tools import StatDict


DUMP_FN = 'gene_info.trunc'


class RecordSet(object):
    '''
    Record collector for EntrezGene dumps.
    '''

    ambig_unit = "terms"

    def __init__(self, fn=DUMP_FN, collect_stats=False, oidgen=None):
        self.fn = fn
        self.stats = StatDict()
        self.collect_stats = collect_stats
        if oidgen is None:
            oidgen = Base36Generator()
        self.oidgen = oidgen

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        with open(self.fn) as f:
            for row in csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE):
                if row['Symbol'] == 'NEWENTRY':
                    continue

                oid = next(self.oidgen)

                terms = set([row['Symbol']])
                synonyms = row['Synonyms']
                if synonyms != '-':
                    terms.update(synonyms.split('|'))

                if self.collect_stats:
                    self.update_stats(len(terms))

                for term in terms:
                    entry = (oid,
                             'EntrezGene',
                             row['GeneID'],
                             term,
                             row['Symbol'],
                             'gene/protein')
                    yield entry

    def update_stats(self, terms_per_id):
        '''
        Update the ambiguity/redundancy statistics.
        '''
        self.stats["ids"] += 1
        self.stats["terms"] += terms_per_id
        self.stats["ratios"][terms_per_id, "terms/id"] += 1

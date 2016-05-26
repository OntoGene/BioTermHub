#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's EntrezGene dump ("gene_info.trunc").
'''


import re
import csv

from termhub.lib.base36gen import Base36Generator
from termhub.lib.tools import StatDict, Fields


DUMP_FN = 'gene_info.trunc'


class RecordSet(object):
    '''
    Record collector for EntrezGene dumps.
    '''

    ambig_unit = "terms"

    def __init__(self, fn=DUMP_FN, collect_stats=False, oidgen=None, mapping=None):
        self.fn = fn
        self.stats = StatDict()
        self.collect_stats = collect_stats
        if oidgen is None:
            oidgen = Base36Generator()
        self.oidgen = oidgen
        self.resource = self.mapping(mapping, 'resource', 'EntrezGene')
        self.entity_type = self.mapping(mapping, 'entity_type', 'gene/protein')

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        with open(self.fn, newline='') as f:
            for row in csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL):
                if row['Symbol'] == 'NEWENTRY':
                    # Skip these (placeholders for future addition?).
                    continue

                oid = next(self.oidgen)

                terms = set([row['Symbol']])
                synonyms = row['Synonyms']
                if synonyms != '-':
                    terms.update(synonyms.split('|'))

                if self.collect_stats:
                    self.update_stats(len(terms))

                for term in terms:
                    entry = Fields(oid,
                                   self.resource,
                                   row['GeneID'],
                                   term,
                                   row['Symbol'],
                                   self.entity_type)
                    yield entry

    @staticmethod
    def mapping(mapping, field, default):
        '''
        Get alternative names for one of the fixed-valued fields.
        '''
        try:
            m = mapping[field]
        except (TypeError, KeyError):
            return default
        try:
            return m[default]
        except KeyError:
            for key in m:
                if re.match(key, default):
                    return m[key]
            return default

    def update_stats(self, terms_per_id):
        '''
        Update the ambiguity/redundancy statistics.
        '''
        self.stats["ids"] += 1
        self.stats["terms"] += terms_per_id
        self.stats["ratios"][terms_per_id, "terms/id"] += 1

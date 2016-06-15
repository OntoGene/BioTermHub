#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parse NCBI's EntrezGene dump ("gene_info.trunc").
'''


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
            reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if row['Symbol'] == 'NEWENTRY':
                    # Skip these (placeholders for future addition?).
                    continue

                if row['Synonyms'] != '-':
                    synonyms = row['Synonyms'].split('|')
                else:
                    synonyms = []

                yield row['GeneID'], row['Symbol'], synonyms

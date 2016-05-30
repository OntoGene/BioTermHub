#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect CTD chemicals and diseases
("CTD_chemicals.csv" and "CTD_diseases.csv").
'''


import csv

from termhub.inputfilters.abc import AbstractRecordSet
from termhub.lib.tools import Fields


CHEM_FN = 'CTD_chemicals.csv'
DIS_FN = 'CTD_diseases.csv'


class RecordSet(AbstractRecordSet):
    '''
    Record collector for ChEBI.
    '''

    ambig_unit = "terms"
    resource = None  # varying values
    entity_type = None  # varying values

    _resource_names = {
        'MESH': 'CTD (MESH)',
        'OMIM': 'CTD (OMIM)',
    }

    def __init__(self, fn, entity_type, mapping=None, exclude=(), **kwargs):
        # Do not give mapping to the superclass, since those fields are not
        # fixed for CTD.
        super().__init__(fn, **kwargs)
        # entity_type is fixed per RecordSet instance (but not resource!).
        self.entity_type = self.mapping(mapping, 'entity_type', entity_type)
        self._resource_mapping = {
            plain: self.mapping(mapping, 'resource', wrapped)
            for plain, wrapped in self._resource_names.items()}
        self._exclude = frozenset(exclude)

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for ns, id_, pref, synonyms in self._iter_concepts():
            oid = next(self.oidgen)

            terms = set(synonyms)
            terms.add(pref)

            if self.collect_stats:
                self.update_stats(len(terms))

            resource = self._resource_mapping[ns]

            for term in terms:
                if (id_, term) not in self._exclude:
                    yield Fields(oid,
                                 resource,
                                 id_,
                                 term,
                                 pref,
                                 self.entity_type)

    def _iter_concepts(self):
        '''
        Parse CSV and extract the relevant information.
        '''
        # The CTD format seems to use the default CSV properties
        # (delimiter comma and minimal quoting with '"').
        reader = csv.reader(self._iter_body())
        for name, id_, _, _, _, _, _, synonyms, _ in reader:
            ns, id_ = id_.split(':')  # split away the namespace prefix
            synonyms = synonyms.split('|') if synonyms else []
            yield ns, id_, name, synonyms

    def _iter_body(self):
        '''
        Iterate over the lines following the header lines.
        '''
        with open(self.fn, newline='') as f:
            for line in f:
                # Skip initial lines until one without leading "#" is found.
                if not line.startswith('#'):
                    yield line
                    break
            yield from f

    @classmethod
    def resource_names(cls):
        return list(cls._resource_names.values())

    @classmethod
    def entity_type_names(cls):
        return ['chemical', 'disease']

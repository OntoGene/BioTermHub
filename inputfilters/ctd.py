#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect CTD chemicals and diseases
("CTD_chemicals.csv" and "CTD_diseases.csv").
'''


import csv
import itertools as it

from termhub.inputfilters._base import IterConceptRecordSet


class RecordSet(IterConceptRecordSet):
    '''
    Abstract record collector for CTD.
    '''

    resource = None  # varying values
    entity_type = None  # fixed value in subclasses

    source_ref = 'http://ctdbase.org/'

    _resource_names = {
        'MESH': 'CTD (MESH)',
        'OMIM': 'CTD (OMIM)',
    }

    def __init__(self, mapping=None, exclude=(), **kwargs):
        # Do not give mapping to the superclass, since the resource field isn't
        # fixed for CTD.
        super().__init__(**kwargs)
        self.entity_type = self.mapping(mapping, 'entity_type', self.entity_type)
        self._resource_mapping = {
            plain: self.mapping(mapping, 'resource', wrapped)
            for plain, wrapped in self._resource_names.items()}
        self._exclude = frozenset(exclude)

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
            synonyms.append(name)
            terms = set(term for term in synonyms
                        if (id_, term) not in self._exclude)
            if terms:
                resource = self._resource_mapping[ns]
                yield id_, name, terms, self.entity_type, resource

    def _iter_body(self):
        '''
        Iterate over the lines following the header lines.
        '''
        with open(self.fn, encoding='utf-8', newline='') as f:
            # Skip initial lines until one without leading "#" is found.
            yield from it.dropwhile(lambda line: line.startswith('#'), f)

    @classmethod
    def resource_names(cls):
        return list(cls._resource_names.values())

    @staticmethod
    def _update_steps():
        return ('gz',)


class ChemRecordSet(RecordSet):
    '''
    Record collector for CTD chemicals.
    '''

    entity_type = 'chemical'

    dump_fn = 'CTD_chemicals.csv'
    remote = 'http://ctdbase.org/reports/CTD_chemicals.csv.gz'

    @classmethod
    def dump_label(cls):
        return 'CTD chemicals'


class DiseaseRecordSet(RecordSet):
    '''
    Record collector for CTD chemicals.
    '''

    entity_type = 'disease'

    dump_fn = 'CTD_diseases.csv'
    remote = 'http://ctdbase.org/reports/CTD_diseases.csv.gz'

    @classmethod
    def dump_label(cls):
        return 'CTD diseases'

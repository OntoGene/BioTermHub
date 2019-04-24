#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect CTD chemicals and diseases
("CTD_chemicals.tsv" and "CTD_diseases.tsv").
'''


import io
import csv
import itertools as it
from collections.abc import Mapping

from ._base import IterConceptRecordSet, UMLSIterConceptMixin


class RecordSet(UMLSIterConceptMixin, IterConceptRecordSet):
    '''
    Abstract record collector for CTD.
    '''

    resource = None  # varying values
    entity_type = None  # fixed value in subclasses
    uri_prefix = {
        'MESH': 'http://id.nlm.nih.gov/mesh/',
        'OMIM': 'http://www.omim.org/entry/',
    }

    source_ref = 'http://ctdbase.org/'
    umls_abb = 'OMIM'            # one target for the UMLS update
    umls_abbs = {'MESH': 'MSH',  # two targets for the CTD update
                 'OMIM': 'OMIM'}

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

    def _prefix_factory(self, prefix):
        if isinstance(prefix, Mapping):
            # Treat the ID prefix differently for MESH and OMIM.
            # Base the distinction on the shape of the IDs.
            mesh, omim = prefix['MESH'], prefix['OMIM']
            def _prefix_id(id_):
                prefix = mesh if id_.startswith(('C', 'D')) else omim
                return prefix + id_
            return _prefix_id
        else:
            return super()._prefix_factory(prefix)

    def _cui_concepts(self):
        for id_, ns, cui, pref, *terms in self._concept_rows():
            terms = set(t for t in terms if (id_, t) not in self._exclude)
            if terms:
                resource = self._resource_mapping[ns]
                yield id_, cui, pref, terms, self.entity_type, resource

    _line_template = '{id}\t{ns}\t{cui}\t{pref}\t{terms}\n'

    @classmethod
    def preprocess(cls, stream):
        '''
        Parse CSV and extract the relevant information.
        '''
        # The CTD format seems to use the default CSV properties
        # (delimiter comma and minimal quoting with '"').
        reader = csv.reader(cls._iter_body(stream))
        cui_maps = {ns: cls._load_cui_map(sab)
                    for ns, sab in cls.umls_abbs.items()}
        for name, id_, _, _, _, _, _, synonyms, _ in reader:
            ns, id_ = id_.split(':')  # split away the namespace prefix
            terms = synonyms.split('|') if synonyms else []
            terms.append(name)
            for cui, terms in cls._assign_cuis(id_, terms, cui_maps[ns]):
                yield cls._canonical_line(
                    id=id_, ns=ns, cui=cui, pref=name, terms=terms)

    @staticmethod
    def _iter_body(stream):
        '''
        Iterate over the lines following the header lines.
        '''
        stream = io.TextIOWrapper(stream, encoding='utf-8', newline='')
        # Skip initial lines until one without leading "#" is found.
        return it.dropwhile(lambda line: line.startswith('#'), stream)

    @classmethod
    def resource_names(cls):
        return list(cls._resource_names.values())

    @classmethod
    def _update_steps(cls):
        return ('gz', cls.preprocess)


class ChemRecordSet(RecordSet):
    '''
    Record collector for CTD chemicals.
    '''

    entity_type = 'chemical'

    dump_fn = 'CTD_chemicals.tsv'
    remote = 'http://ctdbase.org/reports/CTD_chemicals.csv.gz'

    @classmethod
    def dump_label(cls):
        return 'CTD chemicals'


class DiseaseRecordSet(RecordSet):
    '''
    Record collector for CTD chemicals.
    '''

    entity_type = 'disease'

    dump_fn = 'CTD_diseases.tsv'
    remote = 'http://ctdbase.org/reports/CTD_diseases.csv.gz'

    @classmethod
    def dump_label(cls):
        return 'CTD diseases'

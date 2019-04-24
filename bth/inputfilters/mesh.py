#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect MeSH descriptions and supplements ("mesh-{desc,supp}.json.pile").
'''


import json
from collections import namedtuple
from datetime import datetime

from lxml import etree

from ._base import IterConceptRecordSet, UMLSIterConceptMixin
from ..lib.tools import classproperty


# These headings for the initial letter of the MeSH Tree numbers are not given
# anymore in the 2016 release.
# Should we still use them?
TREES = {
    'A': 'Anatomy',
    'B': 'Organisms',
    'C': 'Diseases',
    'D': 'Chemicals and Drugs',
    'E': 'Analytical,Diagnostic and Therapeutic Techniques and Equipment',
    'F': 'Psychiatry and Psychology',
    'G': 'Phenomena and Processes',
    'H': 'Disciplines and Occupations',
    'I': 'Anthropology,Education,Sociology and Social Phenomena',
    'J': 'Technology,Industry,Agriculture',
    'K': 'Humanities',
    'L': 'Information Science',
    'M': 'Named Groups',
    'N': 'Health Care',
    'V': 'Publication Characteristics',
    'Z': 'Geographicals',
}


DescEntry = namedtuple('DescEntry', 'id cui pref terms trees')
SuppEntry = namedtuple('SuppEntry', 'id cui pref terms refs')


class RecordSet(UMLSIterConceptMixin, IterConceptRecordSet):
    '''
    Record collector for MeSH.
    '''

    resource = None  # Not a fixed field.
    entity_type = None  # Not a fixed field.
    uri_prefix = 'http://id.nlm.nih.gov/mesh/'

    dump_fn = ('mesh-desc.json.pile', 'mesh-supp.json.pile')
    _remote = 'ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/xmlmesh/{}{}.gz'
    source_ref = 'https://www.nlm.nih.gov/mesh/meshhome.html'
    umls_abb = 'MSH'

    @classproperty
    def remote(cls):
        '''Keep this attribute up to date for long running processes.'''
        year = datetime.now().year
        return tuple(cls._remote.format(level, year)
                     for level in ('desc', 'supp'))

    tree_type_defaults = {
        'B': 'organism',
        'C': 'disease',
        'D': 'chemical',
    }

    def __init__(self, tree_types=None, mapping=None, **kwargs):
        # Do not give mapping to the superclass, since those fields are not
        # fixed for MeSH.
        super().__init__(**kwargs)
        if tree_types is None:
            tree_types = self.tree_type_defaults
        self._tree_types = tree_types
        self._desc_names = self._resource_name_by_tree('desc', tree_types)
        self._supp_names = self._resource_name_by_tree('supp', tree_types)
        self._resource_mapping = {
            name: self.mapping(mapping, 'resource', name)
            for name in self.resource_names(tree_types)}
        self._entity_type_mapping = {
            name: self.mapping(mapping, 'entity_type', name)
            for name in self.entity_type_names(tree_types)}

    def _cui_concepts(self):
        for entry, tree, resource in self._iter_entries():
            entity_type = self._entity_type_mapping[self._tree_types[tree]]
            resource = self._resource_mapping[resource]
            yield (*entry[:-1], entity_type, resource)

    def _iter_entries(self):
        '''
        Iterate over descriptors and supplementals.
        '''
        ref_trees = {}
        for entry in self._iter_desc():
            trees = set(entry.trees)
            ref_trees[entry.id] = trees
            for tree in trees.intersection(self._tree_types):
                resource = self._desc_names[tree]
                yield entry, tree, resource
        for entry in self._iter_supp():
            trees = set(t for id_ in entry.refs for t in ref_trees[id_])
            for tree in trees.intersection(self._tree_types):
                resource = self._supp_names[tree]
                yield entry, tree, resource

    def _iter_desc(self):
        '''
        Iterate over DescriptorRecord entries.
        '''
        return self._get_json_pile(self.fn[0], DescEntry)

    def _iter_supp(self):
        '''
        Iterate over SupplementalRecord entries.
        '''
        return self._get_json_pile(self.fn[1], SuppEntry)

    @staticmethod
    def _get_json_pile(fn, container):
        '''
        JSON pile: text file with one JSON fragment per line.
        '''
        with open(fn, encoding='ascii') as f:
            for line in f:
                entry = container(*json.loads(line))
                yield entry

    @classmethod
    def _prep_desc(cls, stream):
        '''
        Preprocess DescriptorRecord entries and save them in a JSON pile.
        '''
        for _, record in etree.iterparse(stream, tag='DescriptorRecord'):
            # DescriptorName/String seems to be always the same as
            # .//Term[@RecordPreferredTermYN="Y"]/String,
            # so it's probably safe to use either as preferred term.

            # There's no need to add DescriptorName/String or
            # .//ConceptName/String to the terms set,
            # as these are all included in the .//Term/String nodes.

            yield (
                record.find('DescriptorUI').text,
                record.find('DescriptorName/String').text,
                tuple(set(n.text for n in record.iterfind('.//Term/String'))),
                [n.text[0] for n in record.iterfind('.//TreeNumber')],
            )
            record.clear()

    @classmethod
    def _prep_supp(cls, stream):
        '''
        Preprocess SupplementalRecord entries and save them in a JSON pile.
        '''
        for _, record in etree.iterparse(stream, tag='SupplementalRecord'):
            yield (
                record.find('SupplementalRecordUI').text,
                record.find('SupplementalRecordName/String').text,
                tuple(set(n.text for n in record.iterfind('.//Term/String'))),
                [n.text.lstrip('*') # What does the * mean in ref IDs?
                 for n in record.iterfind('.//DescriptorUI')],
            )
            record.clear()

    @classmethod
    def _json_pile(cls, entries):
        '''
        Serialise tuples to a JSON pile. Also, insert UMLS CUIs.
        '''
        cui_map = cls._load_cui_map()
        for id_, pref, terms, meta in entries:
            for cui, terms in cls._assign_cuis(id_, terms, cui_map):
                line = json.dumps((id_, cui, pref, terms, meta)) + '\n'
                yield line.encode('ascii')

    @classmethod
    def dump_label(cls):
        return 'MeSH'

    @classmethod
    def update_info(cls):
        steps = zip(cls.remote, (cls._prep_desc, cls._prep_supp), cls.dump_fn)
        return [(r, 'gz', prep, cls._json_pile, fn) for r, prep, fn in steps]

    @classmethod
    def resource_names(cls, trees=None):
        if trees is None:
            trees = cls.tree_type_defaults.keys()
        return [name
                for s in ('desc', 'supp')
                for name in cls._resource_name_by_tree(s, trees).values()]

    @staticmethod
    def _resource_name_by_tree(subresource, trees):
        return {t: 'MeSH {} ({})'.format(subresource, TREES[t])
                for t in trees}

    @classmethod
    def entity_type_names(cls, tree_types=None):
        if tree_types is None:
            tree_types = cls.tree_type_defaults
        return list(tree_types.values())

#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect MeSH descriptions and supplements ("desc.xml", "supp.xml").
'''


from collections import namedtuple

from lxml import etree

from termhub.inputfilters.recordset import AbstractRecordSet
from termhub.lib.tools import Fields


DESC_FN = 'desc.xml'
SUPP_FN = 'supp.xml'

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


DescEntry = namedtuple('DescEntry', 'id pref terms trees')
SuppEntry = namedtuple('SuppEntry', 'id pref terms refs')


class RecordSet(AbstractRecordSet):
    '''
    Record collector for MeSH.
    '''

    ambig_unit = "terms"
    resource = None  # Not a fixed field.
    entity_type = None  # Not a fixed field.

    tree_type_defaults = {
        'B': 'organism',  # maybe "species" would be more consistent?
        'C': 'disease',
        'D': 'chemical',
    }

    def __init__(self, desc=DESC_FN, supp=SUPP_FN, tree_types=None,
                 mapping=None, **kwargs):
        # Do not give mapping to the superclass, since those fields are not
        # fixed for MeSH.
        super().__init__((desc, supp), **kwargs)
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

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for entry, tree, resource in self._iter_entries():
            oid = next(self.oidgen)

            if self.collect_stats:
                self.update_stats(len(entry.terms))

            entity_type = self._entity_type_mapping[self._tree_types[tree]]
            resource = self._resource_mapping[resource]

            for term in entry.terms:
                yield Fields(oid,
                             resource,
                             entry.id,
                             term,
                             entry.pref,
                             entity_type)

    def _iter_entries(self):
        '''
        Iterate over descriptors and supplementals.
        '''
        ref_trees = {}
        for entry in self._iter_desc():
            ref_trees[entry.id] = entry.trees
            for tree in entry.trees.intersection(self._tree_types):
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
        for _, record in etree.iterparse(self.fn[0], tag='DescriptorRecord'):
            # DescriptorName/String seems to be always the same as
            # .//Term[@RecordPreferredTermYN="Y"]/String,
            # so it's probably safe to use either as preferred term.

            # There's no need to add DescriptorName/String or
            # .//ConceptName/String to the terms set,
            # as these are all included in the .//Term/String nodes.

            entry = DescEntry(
                record.find('DescriptorUI').text,
                record.find('DescriptorName/String').text,
                set(n.text for n in record.iterfind('.//Term/String')),
                set(n.text[0] for n in record.iterfind('.//TreeNumber')),
            )
            record.clear()
            yield entry

    def _iter_supp(self):
        '''
        Iterate over SupplementalRecord entries.
        '''
        for _, record in etree.iterparse(self.fn[1], tag='SupplementalRecord'):
            entry = SuppEntry(
                record.find('SupplementalRecordUI').text,
                record.find('SupplementalRecordName/String').text,
                set(n.text for n in record.iterfind('.//Term/String')),
                set(n.text.lstrip('*') # What does the * mean in ref IDs?
                    for n in record.iterfind('.//DescriptorUI')),
            )
            record.clear()
            yield entry

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

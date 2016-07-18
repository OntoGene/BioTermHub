#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Gene Ontology entries ("go.obo").
'''


from termhub.inputfilters._obo import OboRecordSet
from termhub.lib.tools import Fields


class RecordSet(OboRecordSet):
    '''
    Record collector for Gene Ontology.
    '''

    ambig_unit = "terms"
    resource = 'Gene Ontology'
    entity_type = None  # Not a fixed field.

    dump_fn = 'go.obo'
    remote = 'http://purl.obolibrary.org/obo/go.obo'

    entity_type_defaults = ('biological_process',
                            'cellular_component',
                            'molecular_function')

    def __init__(self, entity_types=None, mapping=None, **kwargs):
        # Do not give mapping to the superclass, since the entity_type
        # field is not fixed.
        super().__init__(**kwargs)
        if entity_types is None:
            entity_types = self.entity_type_defaults
        self._entity_types = entity_types
        self.resource = self.mapping(mapping, 'resource', self.resource)
        self._entity_type_mapping = {
            name: self.mapping(mapping, 'entity_type', name)
            for name in self._entity_types}

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for concept in self._iter_stanzas():
            oid = next(self.oidgen)

            terms = concept['synonyms']
            terms.add(concept['pref'])

            if self.collect_stats:
                self.update_stats(len(terms))

            entity_type = self._entity_type_mapping[concept['entity_type']]

            for term in terms:
                entry = Fields(oid,
                               self.resource,
                               concept['id'],
                               term,
                               concept['pref'],
                               entity_type)
                yield entry

    @classmethod
    def entity_type_names(cls):
        '''
        Get a list of possible entity_type names.
        '''
        return list(cls.entity_type_defaults)
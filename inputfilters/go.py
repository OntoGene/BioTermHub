#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Gene Ontology entries ("go.obo").
'''


from termhub.inputfilters._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Gene Ontology.
    '''

    ambig_unit = "terms"
    resource = 'Gene Ontology'
    entity_type = None  # Not a fixed field.

    dump_fn = 'go.obo'
    remote = 'http://purl.obolibrary.org/obo/go.obo'
    source_ref = 'http://geneontology.org/'

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

    def _get_entity_type(self, concept):
        '''
        Perform eventual entity type renaming.
        '''
        return self._entity_type_mapping[concept['entity_type']]

    @classmethod
    def entity_type_names(cls):
        '''
        Get a list of possible entity_type names.
        '''
        return list(cls.entity_type_defaults)

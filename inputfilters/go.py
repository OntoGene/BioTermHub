#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Gene Ontology entries ("go.tsv").
'''


from termhub.inputfilters._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Gene Ontology.
    '''

    resource = 'Gene Ontology'
    entity_type = None  # Not a fixed field.

    dump_fn = 'go.tsv'
    remote = 'http://purl.obolibrary.org/obo/go.obo'
    source_ref = 'http://geneontology.org/'

    entity_type_defaults = ('biological_process',
                            'cellular_component',
                            'molecular_function')

    def __init__(self, entity_types=None, mapping=None, **kwargs):
        '''
        Parameter entity_types: Limit records to the specified
        subset of entity types. Use the default spelling, even
        if they are renamed through `mapping`.
        '''
        # Do not give mapping to the superclass, since the entity_type
        # field is not fixed.
        super().__init__(**kwargs)
        if entity_types is None:
            entity_types = self.entity_type_defaults
        elif isinstance(entity_types, str):
            entity_types = [entity_types]

        self.resource = self.mapping(mapping, 'resource', self.resource)
        self._entity_type_mapping = {
            name: self.mapping(mapping, 'entity_type', name)
            for name in entity_types
        }

    def _iter_concepts(self):
        for id_, entity_type, pref, *terms in self._concept_rows():
            entity_type = self._entity_type_mapping.get(entity_type)
            if entity_type is not None:
                yield id_, pref, terms, entity_type, self.resource

    _line_template = '{id}\t{entity_type}\t{pref}\t{terms}\n'

    @classmethod
    def entity_type_names(cls):
        '''
        Get a list of possible entity_type names.
        '''
        return list(cls.entity_type_defaults)

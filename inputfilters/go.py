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

    resource = 'Gene Ontology'
    entity_type = None  # Not a fixed field.

    dump_fn = 'go.obo'
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
        self._continue_signal = self.ContinueSignal()  # avoid repeated inst.

    def _iter_concepts(self):
        for concept in self._iter_stanzas():
            try:
                yield self._concept_tuple(concept)
            except self.ContinueSignal:
                # Ignored entity type.
                pass

    def _get_entity_type(self, concept):
        '''
        Perform eventual entity type renaming.
        '''
        entity_type = concept['entity_type']
        try:
            return self._entity_type_mapping[entity_type]
        except KeyError:
            raise self._continue_signal

    @classmethod
    def entity_type_names(cls):
        '''
        Get a list of possible entity_type names.
        '''
        return list(cls.entity_type_defaults)

    class ContinueSignal(Exception):
        '''
        Skip this record.
        '''

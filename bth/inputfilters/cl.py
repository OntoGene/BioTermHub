#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Cell Ontology entries ("cl.tsv").
'''


from ._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Cell Ontology.
    '''

    resource = 'Cell Ontology'
    entity_type = 'cell'

    dump_fn = 'cl.tsv'
    remote = 'http://purl.obolibrary.org/obo/cl.obo'
    source_ref = 'http://obofoundry.org/ontology/cl.html'

    @classmethod
    def iter_stanzas(cls, stream):
        # Wrap the superclass method for excluding non-cell records.
        for concept in super().iter_stanzas(stream):
            if concept['id'].startswith('CL:'):
                yield concept

#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Cell Ontology entries ("cl.obo").
'''


from termhub.inputfilters._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Cell Ontology.
    '''

    ambig_unit = "terms"
    resource = 'Cell Ontology'
    entity_type = 'cell'

    dump_fn = 'cl.obo'
    remote = 'http://purl.obolibrary.org/obo/cl.obo'
    source_ref = 'http://obofoundry.org/ontology/cl.html'

    def _iter_stanzas(self):
        '''
        Wrap the superclass method for excluding non-cell records.
        '''
        for concept in super()._iter_stanzas():
            if concept['id'].startswith('CL:'):
                yield concept

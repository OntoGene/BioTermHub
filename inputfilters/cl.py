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

    # TODO: maybe exclude entries with namespace != 'cell'

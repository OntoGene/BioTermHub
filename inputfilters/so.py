#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Sequence Ontology entries ("so.obo").
'''


from termhub.inputfilters._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Sequence Ontology.
    '''

    ambig_unit = "terms"
    resource = 'Sequence Ontology'
    entity_type = 'sequence'

    dump_fn = 'so.obo'
    remote = ('https://raw.githubusercontent.com/The-Sequence-Ontology/'
              'SO-Ontologies/master/so.obo')
    source_ref = 'http://www.sequenceontology.org/'

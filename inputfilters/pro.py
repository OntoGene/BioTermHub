#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Protein Ontology entries ("pro.obo").
'''


from termhub.inputfilters._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Protein Ontology.
    '''

    ambig_unit = "terms"
    resource = 'Protein Ontology'
    entity_type = 'gene/protein'
    dump_fn = 'pro.obo'

    # TODO: maybe exclude entries with namespace != 'gene'

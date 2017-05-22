#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Protein Ontology entries ("pro_nonreasoned.obo").
'''


from termhub.inputfilters._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Protein Ontology.
    '''

    ambig_unit = "terms"
    resource = 'Protein Ontology'
    entity_type = 'gene/protein'

    dump_fn = 'pro_nonreasoned.obo'
    remote = ('ftp://ftp.pir.georgetown.edu/databases/ontology/pro_obo/'
              'pro_nonreasoned.obo')
    source_ref = 'http://pir20.georgetown.edu/pro/'

    # TODO: maybe exclude entries with namespace != 'gene'

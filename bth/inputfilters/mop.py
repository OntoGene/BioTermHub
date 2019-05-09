#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2019


'''
Collect Molecular Process Ontology entries ("mop.tsv").
'''


from ._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for the Molecular Process Ontology.
    '''

    resource = 'Molecular Process Ontology'
    entity_type = 'molecular_process'

    dump_fn = 'mop.tsv'
    remote = 'http://purl.obolibrary.org/obo/mop.obo'
    source_ref = 'https://www.ebi.ac.uk/ols/ontologies/mop'

    @classmethod
    def iter_stanzas(cls, stream):
        # Wrap the superclass method for excluding non-MOP concepts.
        for concept in super().iter_stanzas(stream):
            if concept['id'].startswith('MOP:'):
                yield concept

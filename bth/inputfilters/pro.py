#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect Protein Ontology entries ("pro_nonreasoned.tsv").
'''


from ._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Protein Ontology.
    '''

    resource = 'Protein Ontology'
    entity_type = 'gene/protein'

    dump_fn = 'pro_nonreasoned.tsv'
    remote = ('ftp://ftp.pir.georgetown.edu/databases/ontology/pro_obo/'
              'pro_nonreasoned.obo')
    source_ref = 'http://pir20.georgetown.edu/pro/'

    @classmethod
    def _iter_stanzas(cls, stream):
        '''
        Wrap the superclass method for excluding non-gene records.
        '''
        for concept in super()._iter_stanzas(stream):
            if concept['id'].startswith('PR:'):
                # This is the bulk of the data.
                yield concept
            elif (concept.get('entity_type') == 'gene'
                  and not concept['id'].startswith('NCBIGene:')):
                # Include genes, except for Entrez Gene (provided separately).
                yield concept

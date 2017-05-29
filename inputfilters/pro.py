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

    resource = 'Protein Ontology'
    entity_type = 'gene/protein'

    dump_fn = 'pro_nonreasoned.obo'
    remote = ('ftp://ftp.pir.georgetown.edu/databases/ontology/pro_obo/'
              'pro_nonreasoned.obo')
    source_ref = 'http://pir20.georgetown.edu/pro/'

    def _iter_stanzas(self):
        '''
        Wrap the superclass method for excluding non-gene records.
        '''
        for concept in super()._iter_stanzas():
            if concept['id'].startswith('PR:'):
                # This is the bulk of the data.
                # For some reason, PR stanzas don't have a namespace.
                yield concept
            elif concept['id'].startswith('NCBIGene:'):
                # Skip Entrez Gene -- we provide it separately already.
                continue
            elif concept['entity_type'] == 'gene':
                # Include all other genes.
                yield concept

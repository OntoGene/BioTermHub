#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Collect Uberon organ/tissue entries ("uberon.basic.tsv").
'''


from termhub.inputfilters._obo import OboRecordSet


class RecordSet(OboRecordSet):
    '''
    Record collector for Uberon.
    '''

    resource = 'Uberon'
    entity_type = 'organ/tissue'

    dump_fn = 'uberon.basic.tsv'
    remote = 'http://purl.obolibrary.org/obo/uberon/basic.obo'
    source_ref = 'http://obofoundry.org/ontology/uberon.html'

    @classmethod
    def _iter_stanzas(cls, stream):
        for concept in super()._iter_stanzas(stream):
            # Suppress entries without a "name" field.
            if 'pref' in concept:
                yield concept

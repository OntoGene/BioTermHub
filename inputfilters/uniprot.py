#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect UniProt proteins ("uniprot_sprot.xml").
'''


from lxml import etree

from termhub.inputfilters.recordset import AbstractRecordSet
from termhub.lib.tools import Fields


DUMP_FN = 'uniprot_sprot.xml'


class RecordSet(AbstractRecordSet):
    '''
    Record collector for UniProt.
    '''

    ambig_unit = "terms"
    resource = 'Swiss-Prot'
    entity_type = 'gene/protein'

    def __init__(self, fn=DUMP_FN, **kwargs):
        super().__init__(fn, **kwargs)

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for id_, pref, terms in self._iter_concepts():
            oid = next(self.oidgen)

            if self.collect_stats:
                self.update_stats(len(terms))

            for term in terms:
                entry = Fields(oid,
                               self.resource,
                               id_,
                               term,
                               pref,
                               self.entity_type)
                yield entry

    def _iter_concepts(self):
        '''
        Extract the relevant information.
        '''
        # Precompose the Xpaths including namespace.
        entry_tag = self._ns('entry')
        id_tag = self._ns('accession')
        pref_tag = self._ns('protein', 'recommendedName', 'fullName')
        syn_tags = [self._ns('protein', status, length)
                    for status in ('recommendedName', 'alternativeName')
                    for length in ('fullName', 'shortName')]

        for _, entry in etree.iterparse(self.fn, tag=entry_tag):
            id_ = entry.find(id_tag).text
            pref = entry.find(pref_tag).text
            synonyms = set(s.text
                           for t in syn_tags
                           for s in entry.iterfind(t))
            yield id_, pref, synonyms

    @staticmethod
    def _ns(*tags):
        '''
        Prepend the namespace prefix to each tag name.
        '''
        ns = '{http://uniprot.org/uniprot}'
        return '/'.join('{}{}'.format(ns, tag) for tag in tags)

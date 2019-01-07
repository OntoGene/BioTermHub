#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect UniProt proteins ("uniprot_sprot.xml").
'''


from lxml import etree

from ._base import IterConceptRecordSet


class RecordSet(IterConceptRecordSet):
    '''
    Record collector for UniProt.
    '''

    resource = 'Swiss-Prot'
    entity_type = 'gene/protein'

    dump_fn = 'uniprot_sprot.tsv'
    remote = ('ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/'
              'knowledgebase/complete/uniprot_sprot.xml.gz')
    source_ref = 'http://web.expasy.org/docs/swiss-prot_guideline.html'

    @classmethod
    def preprocess(cls, stream):
        '''
        Extract the relevant information.
        '''
        # Precompose the Xpaths including namespace.
        entry_tag = cls._ns('entry')
        id_tag = cls._ns('accession')
        pref_tag = cls._ns('protein', 'recommendedName', 'fullName')
        syn_tags = [cls._ns('protein', status, length)
                    for status in ('recommendedName', 'alternativeName')
                    for length in ('fullName', 'shortName')]

        for _, entry in etree.iterparse(stream, tag=entry_tag):
            id_ = cls._untab(entry.find(id_tag).text)
            pref = cls._untab(entry.find(pref_tag).text)
            synonyms = set(cls._untab(s.text)
                           for t in syn_tags
                           for s in entry.iterfind(t))
            entry.clear()
            yield cls._canonical_line(id=id_, pref=pref, terms=synonyms)

    @staticmethod
    def _ns(*tags):
        '''
        Prepend the namespace prefix to each tag name.
        '''
        ns = '{http://uniprot.org/uniprot}'
        return '/'.join('{}{}'.format(ns, tag) for tag in tags)

    @staticmethod
    def _untab(text):
        '''
        Replace tabs and newlines with spaces.
        '''
        return text.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')

    @classmethod
    def _update_steps(cls):
        return ('gz', cls.preprocess)

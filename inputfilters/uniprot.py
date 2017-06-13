#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect UniProt proteins ("uniprot_sprot.xml").
'''


from lxml import etree

from termhub.inputfilters._base import AbstractRecordSet
from termhub.lib.tools import Fields


class RecordSet(AbstractRecordSet):
    '''
    Record collector for UniProt.
    '''

    resource = 'Swiss-Prot'
    entity_type = 'gene/protein'

    dump_fn = 'uniprot_sprot.tsv'
    remote = ('ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/'
              'knowledgebase/complete/uniprot_sprot.xml.gz')
    source_ref = 'http://web.expasy.org/docs/swiss-prot_guideline.html'

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
        Get the pre-extracted information from TSV.
        '''
        with open(self.fn, encoding='utf-8') as f:
            for line in f:
                id_, pref, *terms = line.rstrip('\n').split('\t')
                yield id_, pref, terms

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
            line = '{}\t{}\t{}\n'.format(id_, pref, '\t'.join(synonyms))
            yield line.encode('utf-8')

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
    def update_info(cls):
        return cls._update_info(['gz', cls.preprocess])

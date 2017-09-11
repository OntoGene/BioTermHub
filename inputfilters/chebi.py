#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Collect ChEBI chemicals ("chebi.obo").
'''


import csv
import codecs
from collections import defaultdict

from termhub.inputfilters._base import IterConceptRecordSet


class RecordSet(IterConceptRecordSet):
    '''
    Record collector for ChEBI.
    '''

    resource = 'ChEBI'
    entity_type = 'chemical'

    dump_fn = 'chebi.tsv'
    remote = tuple('ftp://ftp.ebi.ac.uk/pub/databases/chebi/'
                   'Flat_file_tab_delimited/{}.tsv.gz'.format(name)
                   for name in ('compounds', 'names'))
    source_ref = 'https://www.ebi.ac.uk/chebi/'
    languages = ('en', 'la')  # only consider synonyms of these languages

    @classmethod
    def update_info(cls):
        # Special scenario: a single dump file depends on two distinct
        # remote source files (unlike ncbitax, where the dump depends on
        # two members of a remote archive).
        # Combine them with two wrappers which enable sharing an object
        # between the two pipelines.
        comp_src, name_src = cls.remote
        dec = codecs.getreader('utf8')  # stream decoder
        names_container = []  # closure variable to pass around "names" dict
        def load_names(stream):
            'Wrapper for saving the "names" dict.'
            names_container.append(cls._load_names(stream))
        def merge(stream):
            'Wrapper for passing the "names" dict.'
            return cls._merge_comp_names(stream, names_container[0])
        return [(name_src, 'gz', dec, load_names),
                (comp_src, 'gz', dec, merge, cls.dump_fn)]

    @classmethod
    def preprocess(cls, compounds, names):
        '''
        Read compounds and synonymous names from two TSV streams.
        '''
        # This is a convenience method not used by cls.update_info().
        return cls._merge_comp_names(compounds, cls._load_names(names))

    @classmethod
    def _merge_comp_names(cls, compounds, names):
        for compid, chid, pref in cls._select_comp(compounds):
            terms = set(names.get(compid, ()))
            terms.add(pref)
            terms.discard('null')
            if terms:
                if pref == 'null':
                    pref = 'unknown'
                line = '{}\t{}\t{}\n'.format(chid, pref, '\t'.join(terms))
                yield line.encode('utf-8')

    @classmethod
    def _load_names(cls, stream):
        names = defaultdict(list)
        reader = csv.reader(stream, delimiter='\t')
        next(reader)  # skip header line
        for _, compid, _, _, name, _, lang in reader:
            if lang in cls.languages:
                names[compid].append(name)
        return names

    @classmethod
    def _select_comp(cls, stream):
        reader = csv.reader(stream, delimiter='\t')
        next(reader)  # skip header line
        for compid, _, chid, _, _, name, _, _, _, _ in reader:
            yield compid, chid, name

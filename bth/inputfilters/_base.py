#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Base classes for the RecordSet subclasses.
'''


import os
import re
import csv
import logging
from collections import defaultdict

from ..core import settings
from ..lib.tools import Fields


class AbstractRecordSet(object):
    '''
    Record collector abstract base.
    '''

    resource = None
    entity_type = None

    dump_fn = None
    remote = None
    source_ref = None

    def __init__(self, fn=None, mapping=None, idprefix=None):
        self.fn = self._resolve_dump_fns(fn)
        self.prefix_id = self._prefix_factory(idprefix)
        self.resource = self.mapping(mapping, 'resource', self.resource)
        self.entity_type = self.mapping(mapping, 'entity_type', self.entity_type)

    def __iter__(self):
        raise NotImplementedError

    @classmethod
    def dump_label(cls):
        '''
        A label for the scope of this filter.

        In most cases, this is equivalent to the name of the
        underlying resource.

        This is primarily used to label the checkbox list
        in the web GuI.
        '''
        return cls.resource

    @classmethod
    def resource_names(cls):
        '''
        Get a list of possible resource names.
        '''
        return [cls.resource]

    @classmethod
    def entity_type_names(cls):
        '''
        Get a list of possible entity_type names.
        '''
        return [cls.entity_type]

    @staticmethod
    def mapping(mapping, field, default):
        '''
        Get alternative names for one of the fixed-valued fields.
        '''
        try:
            m = mapping[field]
        except (TypeError, KeyError):
            return default
        try:
            return m[default]
        except KeyError:
            for key in m:
                if re.match(key, default):
                    return m[key]
            return default

    @staticmethod
    def _prefix_factory(prefix):
        if prefix is None:
            return lambda id_: id_
        else:
            return lambda id_: prefix+id_

    @classmethod
    def update_info(cls):
        '''
        Resource-specific instructions for the fetch module.

        Return a list of pipelines:
            For each remote address, create a sequence
            resembling a Unix pipeline.
            The first element is the URL, followed by 0..n
            processing steps (eg. decompressing), ending
            with the ultimate file name (base name).
            The steps are either callables or names of
            fetch_remote.Pipeline methods (eg. "gz").

        Convention: use lists for parallelism (multiple
        remotes/dump files, zip/tar forkings), tuples for
        series (sequential steps).
        '''
        return [(cls.remote, *cls._update_steps(), cls.dump_fn,)]

    @staticmethod
    def _update_steps():
        return ()

    @classmethod
    def dump_fns(cls):
        '''
        Return a tuple of the default paths to all dump files.
        '''
        fns = cls._resolve_dump_fns()
        if isinstance(fns, str):
            return (fns,)
        return fns

    @classmethod
    def _resolve_dump_fns(cls, fn=None):
        if fn is None:
            fn = cls._construct_path(settings.path_dumps, cls.dump_fn)
        return fn

    @staticmethod
    def _construct_path(directory, fn):
        if isinstance(fn, str):
            return os.path.join(directory, fn)
        else:
            # fn is a tuple/list/...
            return tuple(os.path.join(directory, n) for n in fn)


class IterConceptRecordSet(AbstractRecordSet):
    '''
    Base class for RecordSet subclasses with a canonical _iter_concepts method.
    '''

    NO_CUI = 'CUI-less'

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for id_, cui, pref, terms, entity_type, resource in self._cui_concepts():
            id_ = self.prefix_id(id_)

            for term in terms:
                entry = Fields(cui,
                               resource,
                               id_,
                               term,
                               pref,
                               entity_type)
                yield entry

    def _cui_concepts(self):
        '''
        Iterate over ID/CUI/pref/terms/type/source sextuples.
        '''
        for id_, *rest in self._iter_concepts():
            yield (id_, self.NO_CUI, *rest)

    def _iter_concepts(self):
        '''
        Iterate over ID/pref/terms/type/source quintuples.
        '''
        for id_, pref, *terms in self._concept_rows():
            yield id_, pref, terms, self.entity_type, self.resource

    def _concept_rows(self):
        with open(self.fn, encoding='utf-8') as f:
            for line in f:
                yield line.rstrip('\n').split('\t')

    # Line template for the canonical iter-concepts format.
    _line_template = '{id}\t{pref}\t{terms}\n'

    @classmethod
    def _canonical_line(cls, terms=(), **kwargs):
        line = cls._line_template.format(terms='\t'.join(terms), **kwargs)
        return line.encode('utf-8')


class UMLSIterConceptMixin:
    '''
    Mix-in for IterConceptRecordSet subclasses with UMLS CUIs.
    '''

    umls_abb = None  # abbreviation of this source used in UMLS (SAB)

    def _cui_concepts(self):
        for id_, cui, pref, *terms in self._concept_rows():
            yield id_, cui, pref, terms, self.entity_type, self.resource

    _iter_concepts = None  # skipped

    _line_template = '{id}\t{cui}\t{pref}\t{terms}\n'

    @classmethod
    def _assign_cuis(cls, id_, terms, mapping):
        grouped = defaultdict(list)
        for term in terms:
            cui = mapping.get((id_, term), cls.NO_CUI)
            grouped[cui].append(term)
        return grouped.items()

    @classmethod
    def _load_cui_map(cls, sab=None):
        cui_map = defaultdict(set)
        try:
            for cui, id_, term in cls._read_cui_map(sab):
                cui_map[id_, term].add(cui)
        except FileNotFoundError:
            logging.warning('CUI map not found (%s)', cls.umls_dump_fn(sab))
        cui_map = {k: '/'.join(cuis) for k, cuis in cui_map.items()}
        return cui_map

    @classmethod
    def _read_cui_map(cls, sab=None):
        with open(cls.umls_dump_fn(sab), 'r', encoding='utf-8') as f:
            yield from csv.reader(f, delimiter='\t', quotechar=None)

    @classmethod
    def umls_dump_fn(cls, sab=None):
        '''
        Path to the CUI-mapping TSV.
        '''
        fn = '{}.tsv'.format(sab or cls.umls_abb)
        return os.path.join(settings.path_umls_maps, fn)

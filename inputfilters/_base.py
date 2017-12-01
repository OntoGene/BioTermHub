#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Base classes for the RecordSet subclasses.
'''


import os
import re

from termhub.core import settings
from termhub.lib.base36gen import Base36Generator
from termhub.lib.tools import StatDict, Fields


class AbstractRecordSet(object):
    '''
    Record collector abstract base.
    '''

    resource = None
    entity_type = None

    dump_fn = None
    remote = None
    source_ref = None

    def __init__(self, fn=None, oidgen=None, mapping=None, idprefix=None,
                 collect_stats=False):
        self.fn = self._resolve_dump_fns(fn)
        self.stats = StatDict()
        self.collect_stats = collect_stats
        self.oidgen = Base36Generator() if oidgen is None else oidgen
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

    def update_stats(self, terms_per_id):
        '''
        Update the ambiguity/redundancy statistics.
        '''
        self.stats["ids"] += 1
        self.stats["terms"] += terms_per_id
        self.stats["ratios"][terms_per_id, "terms/id"] += 1

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
        '''
        return [(cls.remote,) + cls._update_steps() + (cls.dump_fn,)]

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

    def __iter__(self):
        '''
        Iterate over term entries (1 per synonym).
        '''
        for id_, pref, terms, entity_type, resource in self._iter_concepts():
            oid = next(self.oidgen)
            id_ = self.prefix_id(id_)

            if self.collect_stats:
                self.update_stats(len(terms))

            for term in terms:
                entry = Fields(oid,
                               resource,
                               id_,
                               term,
                               pref,
                               entity_type)
                yield entry

    def _iter_concepts(self):
        '''
        Iterate over ID/pref/terms/type/source quintuples.
        '''
        with open(self.fn, encoding='utf-8') as f:
            for line in f:
                id_, pref, *terms = line.rstrip('\n').split('\t')
                yield id_, pref, terms, self.entity_type, self.resource

#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016--2018


'''
Parser for OBO files.
'''


import re
import io

from ._base import IterConceptRecordSet


class OboRecordSet(IterConceptRecordSet):
    '''
    Abstract record collector for OBO dumps.
    '''

    uri_prefix = 'http://purl.obolibrary.org/obo/'

    @staticmethod
    def _prefix_factory(prefix):
        if prefix is None:
            return lambda id_: id_
        else:
            return lambda id_: prefix+id_.replace(':', '_')

    @classmethod
    def preprocess(cls, stream):
        '''
        Parse .obo stanzas and produce extended _iter_concepts format.
        '''
        stream = io.TextIOWrapper(stream, encoding='utf-8')
        for concept in cls._iter_stanzas(stream):
            yield cls._canonical_line(**concept)

    @classmethod
    def _iter_stanzas(cls, stream):
        '''
        Parse the .obo stanzas.

        Do not call list(...) on this method:
        The same object is yielded in every iteration
        (with modified content).
        '''
        tag_value = re.compile(r'(\w+): (.+)')
        synonym_type = re.compile(r'"((?:[^"]|\\")*)" (.+)')

        inside = False
        concept = {}
        for line in stream:
            line = line.strip()
            if not line:
                # Stanza has ended.
                if 'id' in concept:
                    yield concept
                inside = False
                concept.clear()
            elif line == '[Term]':
                # Stanza starts.
                inside = True
                concept['terms'] = set()
            elif line == 'is_obsolete: true':
                continue
            elif inside:
                tag, value = tag_value.match(line).groups()
                if tag == 'id':
                    concept['id'] = value
                elif tag == 'namespace':
                    concept['entity_type'] = value
                elif tag == 'name':
                    concept['pref'] = value
                    concept['terms'].add(value)
                elif tag == 'synonym':
                    synonym, syntype = synonym_type.match(value).groups()
                    if cls._relevant_synonym(syntype):
                        # Unescape quotes.
                        synonym = synonym.replace('\\"', '"')
                        concept['terms'].add(synonym)
        if 'id' in concept:
            # After the final stanza: last yield.
            yield concept

    @classmethod
    def _relevant_synonym(cls, syntype):
        '''
        Subclass hook for filtering by synonym type.
        '''
        return True

    @classmethod
    def _update_steps(cls):
        return (cls.preprocess,)

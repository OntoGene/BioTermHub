#!/usr/bin/env python
# coding: utf8

# Author: Lenz Furrer, 2015


from __future__ import division, unicode_literals, print_function

import sys
import os
import codecs
from contextlib import contextmanager
import argparse


HERE = os.path.dirname(__file__)
BUILDERPATH = os.path.realpath(os.path.join(HERE, '..', 'adrian'))


def main():
    '''
    Run as script.
    '''
    ap = argparse.ArgumentParser(
        description='Run the BioDB resource creation pipeline.')
    ap.add_argument(
        '-t', '--target', required=True,
        help='target file path')
    ap.add_argument(
        '-r', '--resources', nargs='+', required=True,
        help="a sequence of resources (don't forget proper quoting)")
    ap.add_argument(
        '-n', '--rename-resources', nargs='+', type=flat_to_dict, default={},
        help='a mapping for renaming the resource labels. '
             'Format it as a flat sequence of key-value pairs, '
             'with proper quoting (eg. key-1 "value 1" key-2 "value 2")')
    ap.add_argument(
        '-e', '--rename-entity-types', nargs='+', type=flat_to_dict, default={},
        help='a mapping for renaming the resource labels. '
             'The format is the same as for the -n option')
    ap.add_argument(
        '-b', '--read-back', action='store_true',
        help='read back the resource and entity type labels '
             'and store them in a file each')
    args = ap.parse_args()
    renaming = {'resource': args.rename_resources,
                'entity_type': args.rename_entity_types}

    create_resource(args.resources, renaming, args.target, args.read_back)


def create_resource(resources, renaming, target_fn, read_back=False):
    '''
    Run the BioDB resource creation pipeline.
    '''
    try:
        target_fn = os.path.abspath(target_fn)
        with cd(BUILDERPATH):
            if BUILDERPATH not in sys.path:
                sys.path.append(BUILDERPATH)
            import unified_builder as ub
            import biodb_wrapper
            rsc = biodb_wrapper.ub_wrapper(*resources)
            ub.UnifiedBuilder(rsc, target_fn + '.tmp', mapping=renaming)
    except StandardError as e:
        with codecs.open(target_fn + '.log', 'w', 'utf8') as f:
            f.write('{}: {}\n'.format(e.__class__.__name__, e))
    else:
        os.rename(target_fn + '.tmp', target_fn)
        if read_back:
            # Read back resource and entity type names.
            for level in ('resources', 'entity_types'):
                names = sorted(rsc.__getattribute__(level))
                fn = os.path.join(HERE, '{}.identifiers'.format(level))
                with codecs.open(fn, 'w', 'utf8') as f:
                    f.write('\n'.join(names) + '\n')


def flat_to_dict(flat):
    '''
    Turn a flat sequence into a dictionary.
    '''
    return dict(zip(flat[::2], flat[1::2]))


@contextmanager
def cd(newdir):
    '''
    Temporarily change the working directory.
    '''
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)


if __name__ == '__main__':
    main()

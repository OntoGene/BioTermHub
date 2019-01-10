#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Configuration.
'''


import os
import argparse


ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))

def rel(*args):
    '''
    Construct a path relative to the package root.
    '''
    return os.path.join(ROOT, *args)


def main():
    '''
    Run as script: list the settings.
    '''
    ap = argparse.ArgumentParser(
        description='List the current configuration as key: value pairs.')
    ap.add_argument(
        '-d', '--directories', action='store_true',
        help='list directory paths only')
    ap.add_argument(
        '-f', '--files', action='store_true',
        help='list file paths only')
    ap.add_argument(
        '-n', '--name', action='append',
        help='list the setting matching this name (can be repeated)')
    ap.add_argument(
        '-a', '--all', action='store_true',
        help='list all settings (default)')
    ap.add_argument(
        '-v', '--values-only', action='store_true',
        help='suppress keys')
    ap.add_argument(
        '-r', '--relative-paths', nargs='?', const=os.path.curdir,
        metavar='PATH',
        help='list paths relative to the CWD (or to PATH if given)')
    args = ap.parse_args()
    if not (args.directories or args.files or args.name):
        args.all = True  # no scope options == all

    settings = sorted(
        (k, v) for k, v in globals().items()
        if not k.startswith('__')              # no special stuff
        and not k.isupper()                    # no constants
        and isinstance(v, (int, str, tuple)))  # no functions/modules

    if not args.all:
        settings = [
            (k, v) for k, v in settings
            if (args.directories and k.startswith('path'))
            or (args.files and k.endswith('file'))
            or (args.name and k in args.name)
        ]

    if args.relative_paths is not None:
        def relpath(k, v):
            'If v is a path, convert it to relative.'
            if k.startswith('path') or k.endswith('file'):
                v = os.path.relpath(v, args.relative_paths)
            return (k, v)
        settings = [
            relpath(k, v) for k, v in settings
        ]

    if args.values_only:
        template = '{1}'
    else:
        template = '{0}:\t{1}'

    for k, v in settings:
        print(template.format(k, v))


#
# All data written by the services must be under /mnt/system/ now.
#

def scratch(*args):
    'Path relative to /mnt/system/.../scratch/.'
    scr = '/mnt/system/services/httpd/scratch/ontogene/biotermhub'
    return os.path.join(scr, *args)


#
# Downloaded dumps path
#

# Download directory.

path_dumps = scratch('dumps')
path_umls_maps = scratch('dumps', 'umls')

# Do not attempt to update resources if they are younger than min_update_freq.

min_update_freq = 24*60*60  # 1 day (in seconds)

# Do not check for changes more often than min_check_freq.

min_check_freq = 4*60*60  # 4 hours

# Client-side timeout when downloading resource dumps.

timeout = 10  # seconds

# When checking for concurrent updates, ignore temp files older than this.

concurrent_update_dead = 120  # seconds

# When waiting for a concurrent update to finish, wait this long between checks.

concurrent_update_wait_interval = 15  # seconds

# Processing multiple archive members in parallel requires temp-files;
# keep them in memory up to this size.

tempfile_buffer_size = 2**30  # Bytes (0: never write to disk)


#
# Google-books n-grams for the common-words postfilter
#

gen_voc_year_threshold = 1990
gen_voc_occ_threshold = 1e-7
gen_voc_db_file = scratch(
    'dumps', 'googlebooks-1grams-f-{}.tsv.gz'.format(gen_voc_occ_threshold))


#
# Terminology DB builder
#

# Web interface log

server_host = '0.0.0.0'
server_port = 17931

path_log = scratch('log')
log_file = os.path.join(path_log, 'interface.log')
path_update_logs = scratch('dumps', 'updates')

# Location of aggregated termlists for download

path_download = scratch('downloads')


#
# Connection to OGER
#

oger_url = 'https://pub.cl.uzh.ch/projects/ontogene/oger/'

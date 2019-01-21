#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2019


'''
Central entry point to all BTH executables.
'''


import sys
import importlib
from collections import OrderedDict


COMMANDS = OrderedDict([
    ('aggregate',           'bth.core.aggregate'),
    ('server',               'bth.server.server'),
    ('settings',            'bth.core.settings'),
    ('fetch-remote',        'bth.update.fetch_remote'),
    ('fetch-google-ngrams', 'bth.update.fetch_google_ngrams'),
    ('fetch-umls',          'bth.update.fetch_umls'),
    ('extract-umls-cuis',   'bth.update.extract_umls_cuis'),
])


def main():
    '''
    Delegate calls to the appropriate scripts.
    '''
    try:
        # Extract and match the first command-line argument.
        action = _resolve_action(sys.argv[1])
    except LookupError:
        msg = 'usage: {} COMMAND [OPTIONS...]\ncommands: {}'.format(
            sys.argv[0], ', '.join(COMMANDS))
        sys.exit(msg)
    else:
        # Modify sys.argv to not interfere with the commands' argument parsing.
        sys.argv[:2] = [' '.join(sys.argv[:2])]
        action()


def _resolve_action(arg):
    arg = arg.replace('_', '-')
    arg = arg.replace('f-', 'fetch-')
    matches = [cmd for cmd in COMMANDS if cmd.startswith(arg)]
    try:
        (cmd,) = matches
    except ValueError:
        raise LookupError
    name = COMMANDS[cmd]
    module = importlib.import_module(name)
    return module.main


if __name__ == '__main__':
    main()

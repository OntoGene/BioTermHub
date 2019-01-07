#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2019


'''
Central entry point to all BTH executables.
'''


import sys


COMMANDS = [
    'aggregate',
    'settings',
    'fetch-remote',
    'fetch-google-ngrams',
    'extract-umls-cuis',
]


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
    matches = [cmd for cmd in COMMANDS if cmd.startswith(arg)]
    try:
        (cmd,) = matches
    except ValueError:
        raise LookupError
    cmd = cmd.replace('-', '_')
    return globals()[cmd]


def aggregate():
    '''Aggregate from cached dumps.'''
    import bth.core.aggregate
    bth.core.aggregate.main()


def settings():
    '''Show settings.'''
    import bth.core.settings
    bth.core.settings.main()


def fetch_remote():
    '''Update cached dumps.'''
    import bth.update.fetch_remote
    bth.update.fetch_remote.main()


def fetch_google_ngrams():
    '''Update the Google n-grams.'''
    import bth.update.fetch_google_ngrams
    bth.update.fetch_google_ngrams.main()


def extract_umls_cuis():
    '''Update the extracted CUIs.'''
    import bth.update.extract_umls_cuis
    bth.update.extract_umls_cuis.main()


if __name__ == '__main__':
    main()

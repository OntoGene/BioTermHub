#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2019


'''
Central entry point to all BTH executables.
'''


import sys


COMMANDS = [
    'aggregate',
    'server',
    'settings',
    'fetch-remote',
    'fetch-google-ngrams',
    'fetch-umls',
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


def server():
    '''Start a server.'''
    import bth.server.server
    bth.server.server.main()


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


def fetch_umls():
    '''Download a full UMLS release.'''
    import bth.update.fetch_umls
    bth.update.fetch_umls.main()


def extract_umls_cuis():
    '''Update the extracted CUIs.'''
    import bth.update.extract_umls_cuis
    bth.update.extract_umls_cuis.main()


if __name__ == '__main__':
    main()

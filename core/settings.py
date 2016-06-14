#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Configuration.
'''


import os


HERE = os.path.realpath(os.path.dirname(__file__))

def rel(*args):
    '''
    Construct a path relative to this file's location.
    '''
    return os.path.join(HERE, *args)


#
# Downloaded dumps path
#

# Note: If this is changed after the log file is initialized,
# and timestamps and current versions of files are to be kept, make sure to
# create the directory and move the files to the new location BEFORE
# running an update. Otherwise, the old log file will be erased and all sources
# redownloaded by default.

path_dumps = rel('..', 'dumps')

# Force downloads. Ignores previous timestamps from the logfile.
force = False

# Behavior when the change date of a file cannot be retrieved remotely
# Possible values: ask, force, force-fallback, skip
#
# (force-fallback: Don't attempt download from url with date placeholders
# if remote change date check fails, fall back to previous year instead )
rd_fail = "ask"


#
# Terminology DB builder
#

# Paths for output, statistics and files related to batch processing

path_download = rel('..', 'www', 'downloads')
path_stats = rel('..', 'www', 'stats')
path_batch = rel('..', 'www', 'batch')

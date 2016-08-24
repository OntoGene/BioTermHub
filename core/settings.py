#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015
# Modified: Lenz Furrer, 2016


'''
Configuration.
'''


import os


ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))

def rel(*args):
    '''
    Construct a path relative to the package root.
    '''
    return os.path.join(ROOT, *args)


#
# Downloaded dumps path
#

# Download directory.

path_dumps = rel('dumps')

# Do not attempt to update resources if they are younger than min_update_freq.

min_update_freq = 24*60*60  # 1 day (in seconds)

# Do not check for changes more often than min_check_freq.

min_check_freq = 4*60*60  # 4 hours

# Client-side timeout when downloading resource dumps.

timeout = 10  # seconds


#
# Terminology DB builder
#

# Web interface log

log_file = rel('www', 'interface.log')

# Paths for output, statistics and files related to batch processing

path_download = rel('www', 'downloads')
path_stats = rel('www', 'stats')
path_batch = rel('www', 'batch')

# Email credentials.

email_conn = (None,  # email address
              None,  # server URL
              None,  # port
              None,  # user
              None)  # password


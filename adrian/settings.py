#
# dependencies configuration
#

# Download path

# Note: If this is changed after the log file is initialized, 
# and timestamps and current versions of files are to be kept, make sure to
# create the directory and move the files to the new location BEFORE
# running an update. Otherwise, the old log file will be erased and all sources
# redownloaded by default.

dpath = "/mnt/storage/kitt/projects/clontogene/termdb/data/"

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

path_out = '/mnt/storage/kitt/projects/clontogene/termdb/data/'
path_stats = '/mnt/storage/kitt/projects/clontogene/termdb/www/stats/'
path_batch = '/mnt/storage/kitt/projects/clontogene/termdb/www/batch/'

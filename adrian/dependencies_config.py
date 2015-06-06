# dependencies configuration

# Download path

# Note: If this is changed after the log file is initialized, 
# and timestamps and current versions of files are to be kept, make sure to
# create the directory and move the files to the new location BEFORE
# running an update. Otherwise, the old log file will be erased and all sources
# redownloaded by default.

dpath = "data/"

# Force downloads. Ignores previous timestamps from the logfile.
force = False

# Behavior when the change date of a file cannot be retrieved remotely
# Possible values: ask, force, skip
rd_fail = "ask"

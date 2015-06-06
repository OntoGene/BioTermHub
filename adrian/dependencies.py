# Import from standard library
from os import remove, mkdir
from os.path import exists
from time import time
import datetime
import tarfile
from zipfile import ZipFile
import ftplib
import email.utils as eut
from collections import defaultdict
from glob import glob

# Import from non-standard packages
try:
    from wget import download
    from requests import head
except ImportError:
    print "Error: Modules 'wget' and 'requests' needed to download ressources."
    quit()

class RemoteCDateCheckFailed(Exception):
    '''Raised when a remote change date check (either via FTP or HTTP) fails'''

def getdeps(dpath, force = False, rd_fail="ask"):
    
    # Check if download path exists
    if not exists(dpath):
        mkdir(dpath)
        remove('dependencies.log')
    
    # Remove temporary files from cancelled downloads
    tmpfiles = glob("*.tmp")
    for tmpfile in tmpfiles:
        remove(tmpfile)

    # Initialize and populate hash with file dependencies and 
    # previous modification dates (if available)
    dependencies = {}
    dependencies_log_dict = defaultdict(dict)
    
    
    # Open and parse sources file
    try:
        dependencies_source = open('dependencies.sources', 'r')
        for line in dependencies_source:
            
            # Skip lines that are empty or comments (#)
            if line.strip() and line[0] != "#":
                adress = line.rstrip("\n")
                filename_key = line.split("/")[-1].rstrip("\n")
                dependencies[filename_key] = adress
    
    except IOError:
            print "Error: Dependency sources not found"
            quit()
    
    
    # Open and parse log file if available
    try:
        dependencies_log = open('dependencies.log', 'r')
        try:
            for line in dependencies_log:
                llist = line.rstrip("\n").split(" ")
                dependencies_log_dict[llist[0]]["timestamp"] = \
                    llist[1]
                dependencies_log_dict[llist[0]]["datetime"] = \
                    datetime.date.fromtimestamp(int(llist[1]))
            dependencies_log.close()
        except IndexError:
            dependencies_log.close()
    except IOError:
        pass
        
    # Reopen log file, overwriting old log file
    dependencies_log = open('dependencies.log', 'w')
        
    
    # Iterate through sources 
    for dfile in dependencies:
        dfile_download = False
        force_file = False
        changedate = datetime.date(1970,1,1)
        
        if not force and dfile in dependencies_log_dict:
            print "%-35s\tChecking for a newer version ... " % dfile, 
        else:
            print "%-35s\tFetching timestamp..." % dfile,
        
        # Attempt to fetch timestamp depending on the used protocol
        try:
            if dependencies[dfile].startswith("http"):
                changedate = date_modified_http(dfile, dependencies)
        
            elif dependencies[dfile].startswith("ftp"):
                changedate = date_modified_ftp(dfile, dependencies)
        
        # Handle failed attempts to fetch timestamp
        except RemoteCDateCheckFailed:
            
            # Ask user whether to download or skip
            if rd_fail == "ask":
                usr_input = None
                while usr_input not in ["y", "n"]:
                    usr_input = raw_input("Timestamp of remote file unknown. Download file? (y/n) ")
                if usr_input == "y":
                    force_file = True
                else:
                    continue
            
            # Automatically skip file
            elif rd_fail == "skip":
                continue
            
            # Automatically force download
            elif rd_fail == "force":
                force_file = True

        
        
        if not force:
            # If the file has a log entry, calculate difference in timestamps
            try:
                prev_changedate = dependencies_log_dict[dfile]["datetime"]
                changedate_delta = (changedate - prev_changedate).days
                dfile_is_old = changedate_delta > 0
                dfile_does_not_exist = False
                
            # Else assume the file has not been downloaded before (no local check is performed)
            except KeyError:
                dfile_is_old = False
                dfile_does_not_exist = True
        else:
            dfile_is_old, dfile_does_not_exist = None, None

        
        # Store new timestamp and initiate download to download_path
        if (force or force_file
                  or dfile_is_old 
                  or dfile_does_not_exist):
            
            if dfile_is_old:
                print "new version found, downloading. \n"
            else:
                print "downloading. \n"
            dependencies_log_dict[dfile]["timestamp"] = str(int(unixtimestamp(changedate)))
    
            download_path = dpath + dfile
            download(dependencies[dfile], download_path)
            
            # If the file is compressed, decompress and erase archive
            
            # gzip-compressed tarballs
            if dfile.endsiwth(".tar.gz"):
                tfile = tarfile.open(download_path, "r:gz")
                print "\nExtracting compressed tarball %s ..." % dfile
                tfile.extractall()
                tfile.close()
                remove(download_path)
            
            # gzip-compressed single files
            elif dfile.endswith(".gz"):
                with gzip.open(download_path, "rb") as infile:
                    with open(download_path.rstrip(".gz", "w") as outfile:
                        for line in infile:
                            outfile.write(line)
                remove(download_path)
            
            # ZIP files
            elif dfile.endswith(".zip"):
                print "\nExtracting zip file %s ..." %dfile
                zfile = ZipFile(download_path)
                zfile.extractall()
                zfile.close()
                remove(download_path)
            print ""
    
        else:
            print "up-to-date."
    print "Download complete."
    
    # Write updated dependency log to disk
    for dfile, datedict in dependencies_log_dict.iteritems():
        dependencies_log.write(dfile + " " + datedict["timestamp"] + "\n")
    dependencies_log.close()

def date_modified_ftp(dfile, dependencies):
    '''
    Retrieve modification date for a remote file via FTP
    '''
    dfile_pathlist = dependencies[dfile].split("/")
    dfile_serverpath = dependencies[dfile].split("/", 3)[-1]
    
    # Attempt to retrieve modification date via FTP, 
    # raise RemoteCDateCheckFailed if this fails
    try:
        connection = ftplib.FTP(dfile_pathlist[2])
        connection.login()
        modifiedTime = connection.sendcmd('MDTM ' + dfile_serverpath)
        connection.quit() 
        date_time = datetime.datetime.strptime(modifiedTime[4:], "%Y%m%d%H%M%S")
        date_only = date_time.date()
        return date_only
        
    except ftplib.all_errors as e:
        print "error. "
        raise RemoteCDateCheckFailed
    
def date_modified_http(dfile, dependencies):
    '''
    Retrieve modification date for a remote file via HTTP
    '''
    request = head(dependencies[dfile])
    # Attempt to look up modification date in the HTTP header dictionary, 
    # raise RemoteCDateCheckFailed if this fails
    try:
        header_date = request.headers['last-modified']
        date_time = parsedate(header_date)
        date_only = date_time.date()
        return date_only
    except KeyError:
        print "error. "
        raise RemoteCDateCheckFailed

def parsedate(text):
    '''
    Parse RFC 822 Date string using email.utils, convert to datetime
    (http://stackoverflow.com/a/1472336)
    '''
    return datetime.datetime(*eut.parsedate(text)[:6])

def unixtimestamp(date):
    '''
    Calculate unix timestamp in seconds.
    '''
    return (date - datetime.date(1970,1,1)).total_seconds()

if __name__ == "__main__":
    from dependencies_config import *
    getdeps(dpath, force, rd_fail)

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
import socket # timeout and adress resolution error for FTP
import sys

TIMEOUT = 1

# Import from non-standard packages
try:
    import requests
    from requests import head, ConnectionError
    from requests.adapters import ConnectTimeout
    from progressbar import *
except ImportError:
    print "Error: Modules 'progressbar' and 'requests' needed to download ressources."
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
        except IndexError:
            pass
    except IOError:
        pass        
    finally:
        dependencies_log.close()
        
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
        
        except ConnectTimeout:
            print "error: HTTP connection timed out, skipping ..."
            continue
        
        except ConnectionError:
            print "error: HTTP connection error, skipping ..."
            continue
        
        except socket.timeout:
            print "error: FTP connection timed out, skipping ..."
            continue
        
        except socket.error:
            print "error: FTP connection error, skipping ..."
            continue
        
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
            
            download_path = dpath + dfile
            try:
                if dependencies[dfile].startswith("http"):
                    download_file_http(dependencies[dfile], dpath)
                elif dependencies[dfile].startswith("ftp"):
                    download_file_ftp(dependencies[dfile], dpath)
                    
                # Do not insert timestamp or overwrite previous timestamp if download is forced
                if not force_file:
                    dependencies_log_dict[dfile]["timestamp"] = str(int(unixtimestamp(changedate)))
            except IOError:
                print "Error: Could not download %s" % dfile
                del dependencies_log_dict[dfile]
                continue
            
            # If the file is compressed, decompress and erase archive
            
            # gzip-compressed tarballs
            if dfile.endswith(".tar.gz"):
                tfile = tarfile.open(download_path, "r:gz")
                print "\nExtracting compressed tarball %s ..." % dfile
                tfile.extractall()
                tfile.close()
                remove(download_path)
            
            # gzip-compressed single files
            elif dfile.endswith(".gz"):
                with gzip.open(download_path, "rb") as infile:
                    download_path_stripped = download_path.rstrip(".gz", "w") 
                    with open(download_path_stripped) as outfile:
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
    
    # Reopen log file, overwriting old log file
    dependencies_log = open('dependencies.log', 'w')

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
        connection = ftplib.FTP(dfile_pathlist[2], timeout=TIMEOUT)
        connection.login()
        modifiedTime = connection.sendcmd('MDTM ' + dfile_serverpath)
        connection.quit() 
        date_time = datetime.datetime.strptime(modifiedTime[4:], "%Y%m%d%H%M%S")
        date_only = date_time.date()
        return date_only
        
    except (ftplib.error_perm, ftplib.error_temp) as e:
       print "error. "
       raise RemoteCDateCheckFailed
    
def date_modified_http(dfile, dependencies):
    '''
    Retrieve modification date for a remote file via HTTP
    '''
    request = head(dependencies[dfile], timeout=TIMEOUT)
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
        
# Sources:
# http://stackoverflow.com/a/16696317
# http://stackoverflow.com/a/15645088
def download_file_http(url, path):
    filename = url.split('/')[-1]
    r = requests.get(url, stream=True)
    r.raise_for_status() #throw exception on HTTP error codes
    total_length = r.headers.get('content-length')
    
    if total_length is None: # no content length header
            f.write(response.content)
    else:
        dl = 0
        total_length = int(total_length)
    with open(path + filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                dl += len(chunk)
                f.write(chunk)
                f.flush()
                done = int(50 * dl / total_length)
                sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )    
                sys.stdout.flush()

def download_file_ftp(url, path):
        dfile_pathlist = url.split("/")
        dfile_serverpath = url.split("/", 3)[-1]
        filename = url.split('/')[-1]
        try:
            connection = ftplib.FTP(dfile_pathlist[2], timeout=TIMEOUT)
            connection.login()
            filesize = connection.size(dfile_serverpath)
            print "Downloading %s-%d-bytes" % (filename, filesize)
            
            pbar=ProgressBar(widgets=[FileTransferSpeed(),' ', Bar(marker=RotatingMarker()), ' ', 
                                                    Percentage(),' ', ETA()], maxval=filesize).start()
            # Closure to access pbar
            def handleupload(block):
                pbar.update(pbar.currval+len(block))
                
            with open(path + filename, 'wb') as localfile:
                connection.retrbinary('RETR %s' % dfile_serverpath, callback = handleupload, blocksize = 1024)
            pbar.finish()
            print "Finished"
            connection.quit()
        except (ftplib.error_perm, ftplib.error_temp), self.exc:
            print self.exc

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

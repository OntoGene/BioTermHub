# Import from standard library
from os import remove, rename, mkdir
from os.path import exists
import datetime
import tarfile
from zipfile import ZipFile
import gzip
import ftplib
import email.utils as eut
from collections import defaultdict, OrderedDict
from glob import glob
import socket # timeout and adress resolution error for FTP
import re

TIMEOUT = 1

# Import from non-standard packages
try:
    import requests
    from requests import head, ConnectionError
    try:
        # requests >= 2.7.0
        from requests.adapters import ConnectTimeout
    except ImportError:
        # requests < 2.7.0
        from requests.adapters import Timeout
    from progressbar import *
except ImportError:
    print "Error: Packages 'progressbar' and 'requests' required to download ressources."
    quit()

# Import preprocessors
import ncbi_preprocess
import taxdump_preprocess

class RemoteCDateCheckFailed(Exception):
    '''Raised when a remote change date check (either via FTP or HTTP) fails'''
    
class DownloadFailed(Exception):
    '''Raised when a file download (either via FTP or HTTP) fails'''

def getdeps(dpath, force = False, rd_fail="ask"):
    
    # Check if download path exists
    if not exists(dpath):
        mkdir(dpath)
        if exists('dependencies.log'):
            remove('dependencies.log')
    
    # Remove temporary files from cancelled downloads
    tmpfiles = glob("*.tmp")
    for tmpfile in tmpfiles:
        remove(tmpfile)

    # Initialize and populate hash with file dependencies and 
    # previous modification dates (if available)
    dependencies = OrderedDict()
    dependencies_log_dict = defaultdict(dict)
    preproc_jobs = {}
    preprocessors = {}
    preprocessors["ncbi"] = {"module": ncbi_preprocess, "args":(dpath+"gene_info", dpath+"gene_info.trunc", [1, 2, 4])}
    preprocessors["taxdump"] = {"module": taxdump_preprocess, "args":(dpath+"names.dmp", dpath+"nodes.dmp", dpath+"names.dmp.trunc")}


    # Open and parse sources file
    try:
        dependencies_source = open('dependencies.sources', 'r')
        for line in dependencies_source:
            stripped_line = line.strip()
            # Skip lines that are empty or comments (#)
            if stripped_line and stripped_line[0] != "#":
                try:
                    line, preprocessor = line.split(" | ")
                except ValueError:
                    preprocessor = None
                adress = line.rstrip("\n")
                filename_key = line.split("/")[-1].rstrip("\n")
                dependencies[filename_key] = adress
                if preprocessor:
                    preproc_jobs[filename_key] = preprocessor.rstrip()

        dependencies_source.close()
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
        dependencies_log.close()
    except IOError:
        pass
        
    # Iterate through raw sources 
    for dfile in dependencies:
        
        # Initialize variables to default values
        dfile_download = False
        force_file = False
        changedate = datetime.date(1970,1,1)
        
        # Resolve date substitution strings if present
        
        res_dfile, res_dfile_url, year = resolveurl(dependencies, dfile)
        
        if not force and res_dfile in dependencies_log_dict:
            print "%-35s\tChecking for a newer version ... " % res_dfile,
        else:
            print "%-35s\tFetching timestamp..." % res_dfile,
        
        # Attempt to fetch timestamp depending on the used protocol
        try:
            changedate = fetch_changedate(res_dfile_url)
            
        # Handle failed attempts to fetch timestamp
        except RemoteCDateCheckFailed:
            
            # Ask user whether to download or skip
            if rd_fail == "ask":
                usr_input = None
                while usr_input not in ["d", "p", "s"]:
                    usr_input = raw_input("Timestamp of remote file unknown. (d)ownload file, try (p)revious year or (s)kip? (d/p/s) ")
                
                if usr_input == "d":
                    force_file = True
                
                elif usr_input == "p":
                    # If the url has a date substitution string, attempt to download file with one year subtracted from the current year
                    if year:
                        print "Attempting to fetch timestamp from previous year...",
                        try:
                            del res_dependencies[res_dfile]
                            res_dfile, res_dfile_url, year = resolveurl(dependencies[dfile], yearoffset = 1)
                            changedate = fetch_changedate(res_dfile_url)
                            res_dependencies[res_dfile] = res_dfile_url
                        except RemoteCDateCheckFailed:
                            print "Skipping %s..." % res_dfile
                            continue
                    else:
                        print "No date substitution string found, trying to force download ..."
                        force_file = True
                else:
                    print "Skipping %s..." % res_dfile
                    continue
            
            # Automatically skip file
            elif rd_fail == "skip":
                print "Skipping %s..." % res_dfile
                continue
                
            # Automatically fall-back if url contains a resolved date place-holder
            # Force download if check still fails
            elif rd_fail == "force-fallback":
                try:
                    res_dfile, res_dfile_url, _ = resolveurl(dependencies[dfile], yearoffset = 1)
                    print "Attempting to fetch timestamp from previous year...",
                    changedate = fetch_changedate(res_dfile_url)
                except RemoteCDateCheckFailed:
                    force_file = True
            
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
                prev_changedate = dependencies_log_dict[res_dfile]["datetime"]
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
            elif force_file:
                print "downloading (forced)."
            else:
                print "downloading. \n"
            
            download_path = dpath + res_dfile
            try:
                if res_dfile_url.startswith("http"):
                    download_file_http(res_dfile_url, dpath)
                elif res_dfile_url.startswith("ftp"):
                    download_file_ftp(res_dfile_url, dpath)
                    
                # Do not insert timestamp or overwrite previous timestamp if download is forced
                if not force_file:
                    dependencies_log_dict[res_dfile]["timestamp"] = str(int(unixtimestamp(changedate)))
            except DownloadFailed:
                print "Error: Could not download %s" % res_dfile
                del dependencies_log_dict[res_dfile]
                continue
            
            # If the file is compressed, decompress and erase archive
            
            archive_members = []
            compressed = False
            
            # gzip-compressed tarballs
            if res_dfile.endswith(".tar.gz"):
                compressed = True
                tfile = tarfile.open(download_path, "r:gz")
                print "\nExtracting compressed tarball %s ..." % res_dfile
                archive_members = tfile.getnames()
                tfile.extractall(dpath)
                tfile.close()
                remove(download_path)
            
            #gzip-compressed single files
            elif res_dfile.endswith(".gz"):
                compressed = True
                print "\nExtracting gzipped file %s ..." % res_dfile
                with gzip.open(download_path, "rb") as infile:
                    download_path_stripped = download_path.rstrip(".gz")
                    with open(download_path_stripped, "w") as outfile:
                        for line in infile:
                            outfile.write(line)
                remove(download_path)
            
            #ZIP files
            elif res_dfile.endswith(".zip"):
                compressed = True
                print "\nExtracting zip archive %s ..." %res_dfile
                zfile = ZipFile(download_path)
                archive_members = zfile.namelist()
                zfile.extractall(dpath)
                zfile.close()
                remove(download_path)
            print ""

            # Strip year information from file name
            if year:
                if compressed:
                    for name in archive_members:
                        name_ren = name.replace(year, '')
                        extract_path = dpath + name
                        extract_path_ren = dpath + name_ren
                        rename(extract_path, extract_path_ren)
                else:
                    res_dfile_ren = res_dfile.replace(year, '')
                    download_path_ren = dpath + res_dfile_ren
                    rename(download_path, download_path_ren)
                

            if dfile in preproc_jobs:
                pproc = preproc_jobs[dfile]
                print "Preprocessing %s ..." % preprocessors[pproc]["args"][0]
                preprocessors[pproc]["module"].preprocess(*preprocessors[pproc]["args"])

        else:
            print "up-to-date."
    print "Download complete."
    
    # Reopen log file, overwriting old log file
    dependencies_log = open('dependencies.log', 'w')

    # Write updated dependency log to disk
    for dfile, datedict in dependencies_log_dict.iteritems():
        dependencies_log.write(dfile + " " + datedict["timestamp"] + "\n")
    dependencies_log.close()

def date_modified_ftp(dfile_url):
    '''
    Retrieve modification date for a remote file via FTP
    '''
    dfile_pathlist = dfile_url.split("/")
    dfile_serverpath = dfile_url.split("/", 3)[-1]
    
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
    
def date_modified_http(dfile_url):
    '''
    Retrieve modification date for a remote file via HTTP
    '''
    request = head(dfile_url, timeout=TIMEOUT)
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
        
    pbar = generate_pbar(total_length)
        
    with open(path + filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                dl += len(chunk)
                f.write(chunk)
                f.flush()
                pbar.update(pbar.currval+len(chunk))

def download_file_ftp(url, path):
        dfile_pathlist = url.split("/")
        dfile_serverpath = url.split("/", 3)[-1]
        filename = url.split('/')[-1]
        try:
            connection = ftplib.FTP(dfile_pathlist[2], timeout=TIMEOUT)
            connection.login()
            connection.voidcmd("TYPE I")
            filesize = connection.size(dfile_serverpath)
            print "Downloading %s-%d-bytes" % (filename, filesize)
            
            pbar = generate_pbar(filesize)
            # Closure to access pbar and write localfile
            def handleupload(block):
                pbar.update(pbar.currval+len(block))
                localfile.write(block)
                
            with open(path + filename, 'wb') as localfile:
                connection.retrbinary('RETR %s' % dfile_serverpath, callback = handleupload)
            pbar.finish()
            print "Finished"
            connection.quit()
        except (ftplib.error_perm, ftplib.error_temp), exc:
            raise DownloadFailed

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
    
def resolveurl(dependencies, dfile, yearoffset = 0):
    '''
    Substitute datetime.datetime.strftime-compatible date formatting
    strings with the present date.
    
    yearoffset: year value to subtract from the output of strftime if
    the format string is %Y or %y (4- or 2-digit year)
    '''
    url = dependencies[dfile]
    date_tag = re.match(".*\{(.+?)\}.*", url)
    if date_tag:
        date_string = date_tag.group(1)
        
        dnow = datetime.datetime.now()
        
        year = dnow.strftime(date_string)
        
        if date_string in ("%Y", "%y") and yearoffset:
            try:
                date_subs = int(year)
                year = str(date_subs - yearoffset)
            except ValueError:
                pass
        resolved_url = re.sub("\{.+?\}", year, url)
        resolved_file = resolved_url.split("/")[-1]
        
        return resolved_file, resolved_url, year
        
    else:
        nfile = url.split("/")[-1]
        return dfile, url, False
        
def fetch_changedate(dfile_url):
    if dfile_url.startswith("http"):
        changedate = date_modified_http(dfile_url)

    elif dfile_url.startswith("ftp"):
        changedate = date_modified_ftp(dfile_url)
    
    return changedate
    
def generate_pbar(filesize):
    return ProgressBar(widgets=[FileTransferSpeed(),' ', Bar(marker=RotatingMarker()), ' ', 
                                                    Percentage(),' ', ETA()], maxval=filesize).start()

if __name__ == "__main__":
    from termhub.core.settings import path_dumps as dpath, force, rd_fail
    getdeps(dpath, force, rd_fail)
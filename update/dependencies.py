#!/usr/bin/env python3
# coding: utf8

# Author: Adrian van der Lek, 2015


import re
import os
import glob
import logging
from collections import defaultdict, OrderedDict
from datetime import datetime
import tarfile
from zipfile import ZipFile
import gzip
import ftplib
import email.utils as eut
import socket # timeout and adress resolution error for FTP

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
    logging.exception("Error: Packages 'progressbar' and 'requests' required to download ressources.")
    raise

from termhub.core import settings
from termhub.inputfilters import preprocessors


TIMEOUT = 1


def main():
    '''
    Run as interactive script.
    '''
    getdeps(settings.path_dumps, settings.force, settings.rd_fail)


class RemoteCDateCheckFailed(Exception):
    '''Raised when a remote change date check (either via FTP or HTTP) fails'''

class DownloadFailed(Exception):
    '''Raised when a file download (either via FTP or HTTP) fails'''


def getdeps(dpath, force=False, rd_fail="ask"):
    dep_log = os.path.join(dpath, 'dependencies.log')
    dep_sources = os.path.join(os.path.dirname(__file__),
                               'dependencies.sources')

    # Make sure the download path exists.
    if not os.path.exists(dpath):
        os.mkdir(dpath)

    # Remove temporary files from cancelled downloads.
    for tmpfile in glob.iglob("*.tmp"):
        os.remove(tmpfile)

    # Initialize and populate hash with file dependencies and
    # previous modification dates (if available).
    dependencies = OrderedDict()
    dependencies_log_dict = {}
    preproc_jobs = {}

    # Open and parse sources file.
    with open(dep_sources) as f:
        for line in f:
            line = line.strip()
            # Skip lines that are empty or comments (#)
            if line and line[0] != "#":
                try:
                    address, preprocessor = line.split(" | ")
                except ValueError:
                    address = line
                    preprocessor = None
                filename_key = address.split("/")[-1]
                dependencies[filename_key] = address
                if preprocessor:
                    preproc_jobs[filename_key] = preprocessor

    # Open and parse log file if available.
    try:
        with open(dep_log) as f:
            for line in f:
                key, timestamp = line.split()
                date = datetime.fromtimestamp(int(timestamp))
                dependencies_log_dict[key] = {
                    'timestamp': timestamp,
                    'datetime': date,
                }
    except (IOError, ValueError):
        pass

    # Iterate through raw sources.
    for dfile in dependencies:

        # Initialize variables to default values
        force_file = False
        changedate = datetime(1970, 1, 1, 0, 0)

        # Resolve date substitution strings if present

        res_dfile, res_dfile_url, year = resolveurl(dependencies, dfile)

        if not force and res_dfile in dependencies_log_dict:
            logging.info("%-35s\tChecking for a newer version ...", res_dfile)
        else:
            logging.info("%-35s\tFetching timestamp...", res_dfile)

        # Attempt to fetch timestamp depending on the used protocol
        try:
            changedate = fetch_changedate(res_dfile_url)

        # Handle failed attempts to fetch timestamp
        except RemoteCDateCheckFailed:

            # Ask user whether to download or skip
            if rd_fail == "ask":
                usr_input = None
                while usr_input not in ["d", "p", "s"]:
                    usr_input = input("Timestamp of remote file unknown. (d)ownload file, try (p)revious year or (s)kip? (d/p/s) ")

                if usr_input == "d":
                    force_file = True

                elif usr_input == "p":
                    # If the url has a date substitution string, attempt to download file with one year subtracted from the current year
                    if year:
                        print("Attempting to fetch timestamp from previous year...", end=' ')
                        try:
                            del res_dependencies[res_dfile]
                            res_dfile, res_dfile_url, year = resolveurl(dependencies[dfile], yearoffset=1)
                            changedate = fetch_changedate(res_dfile_url)
                            res_dependencies[res_dfile] = res_dfile_url
                        except RemoteCDateCheckFailed:
                            print("Skipping %s..." % res_dfile)
                            continue
                    else:
                        print("No date substitution string found, trying to force download ...")
                        force_file = True
                else:
                    print("Skipping %s..." % res_dfile)
                    continue

            # Automatically skip file
            elif rd_fail == "skip":
                print("Skipping %s..." % res_dfile)
                continue

            # Automatically fall-back if url contains a resolved date place-holder
            # Force download if check still fails
            elif rd_fail == "force-fallback":
                try:
                    res_dfile, res_dfile_url, _ = resolveurl(dependencies[dfile], yearoffset=1)
                    print("Attempting to fetch timestamp from previous year...", end=' ')
                    changedate = fetch_changedate(res_dfile_url)
                except RemoteCDateCheckFailed:
                    force_file = True

            # Automatically force download
            elif rd_fail == "force":
                force_file = True

        except ConnectTimeout:
            logging.warning("error: HTTP connection timed out, skipping ...")
            continue

        except ConnectionError:
            logging.warning("error: HTTP connection error, skipping ...")
            continue

        except socket.timeout:
            logging.warning("error: FTP connection timed out, skipping ...")
            continue

        except socket.error:
            logging.warning("error: FTP connection error, skipping ...")
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
        if (force
                or force_file
                or dfile_is_old
                or dfile_does_not_exist):

            if dfile_is_old:
                logging.info("new version found, downloading. \n")
            elif force_file:
                logging.info("downloading (forced).")
            else:
                logging.info("downloading. \n")

            download_path = os.path.join(dpath, res_dfile)
            try:
                if res_dfile_url.startswith("http"):
                    download_file_http(res_dfile_url, dpath)
                elif res_dfile_url.startswith("ftp"):
                    download_file_ftp(res_dfile_url, dpath)

                # Do not insert timestamp or overwrite previous timestamp if download is forced
                if not force_file:
                    dependencies_log_dict[res_dfile]["timestamp"] = str(int(changedate.timestamp()))
            except DownloadFailed:
                logging.warning("Error: Could not download %s", res_dfile)
                del dependencies_log_dict[res_dfile]
                continue

            # If the file is compressed, decompress and erase archive

            archive_members = []
            compressed = False

            # gzip-compressed tarballs
            if res_dfile.endswith(".tar.gz"):
                compressed = True
                tfile = tarfile.open(download_path, "r:gz")
                logging.info("Extracting compressed tarball %s ...", res_dfile)
                archive_members = tfile.getnames()
                tfile.extractall(dpath)
                tfile.close()
                os.remove(download_path)

            #gzip-compressed single files
            elif res_dfile.endswith(".gz"):
                compressed = True
                logging.info("\nExtracting gzipped file %s ...", res_dfile)
                with gzip.open(download_path, "rb") as infile:
                    download_path_stripped = download_path.rstrip(".gz")
                    with open(download_path_stripped, "w") as outfile:
                        for line in infile:
                            outfile.write(line)
                os.remove(download_path)

            #ZIP files
            elif res_dfile.endswith(".zip"):
                compressed = True
                logging.info("\nExtracting zip archive %s ...", res_dfile)
                zfile = ZipFile(download_path)
                archive_members = zfile.namelist()
                zfile.extractall(dpath)
                zfile.close()
                os.remove(download_path)

            # Strip year information from file name
            if year:
                if compressed:
                    for name in archive_members:
                        name_ren = name.replace(year, '')
                        extract_path = dpath + name
                        extract_path_ren = dpath + name_ren
                        os.rename(extract_path, extract_path_ren)
                else:
                    res_dfile_ren = res_dfile.replace(year, '')
                    download_path_ren = dpath + res_dfile_ren
                    os.rename(download_path, download_path_ren)


            if dfile in preproc_jobs:
                pproc = preproc_jobs[dfile]
                logging.info("Preprocessing %s ...", dfile)
                preprocessors[pproc]["module"].preprocess(*preprocessors[pproc]["args"])

        else:
            logging.info("up-to-date.")
    logging.info("Download complete.")

    # Write updated dependency log to disk, overwriting old log file.
    with open(dep_log, 'w') as f:
        for dfile, datedict in dependencies_log_dict.items():
            f.write('{} {}\n'.format(dfile, datedict['timestamp']))


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
        date_time = datetime.strptime(modifiedTime[4:], "%Y%m%d%H%M%S")
        return date_time

    except (ftplib.error_perm, ftplib.error_temp):
        logging.exception("error.")
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
        return date_time
    except KeyError:
        logging.exception("error.")
        raise RemoteCDateCheckFailed

# Sources:
# http://stackoverflow.com/a/16696317
# http://stackoverflow.com/a/15645088
def download_file_http(url, path):
    filename = url.split('/')[-1]
    r = requests.get(url, stream=True)
    r.raise_for_status() #throw exception on HTTP error codes
    total_length = r.headers.get('content-length')

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
        logging.info("Downloading %s-%d-bytes", filename, filesize)

        pbar = generate_pbar(filesize)
        # Closure to access pbar and write localfile
        def handleupload(block):
            pbar.update(pbar.currval+len(block))
            localfile.write(block)

        with open(path + filename, 'wb') as localfile:
            connection.retrbinary('RETR %s' % dfile_serverpath, callback=handleupload)
        pbar.finish()
        logging.info("Finished")
        connection.quit()
    except (ftplib.error_perm, ftplib.error_temp) as exc:
        raise DownloadFailed

def parsedate(text):
    '''
    Parse RFC 822 Date string using email.utils, convert to datetime
    (http://stackoverflow.com/a/1472336)
    '''
    return datetime(*eut.parsedate(text)[:6])

def unixtimestamp(date):
    '''
    Calculate unix timestamp in seconds.
    '''
    return (date - datetime.date(1970, 1, 1)).total_seconds()

def resolveurl(dependencies, dfile, yearoffset=0):
    '''
    Substitute datetime.datetime.strftime-compatible date formatting
    strings with the present date.

    yearoffset: year value to subtract from the output of strftime if
    the format string is %Y or %y (4- or 2-digit year)
    '''
    url = dependencies[dfile]
    date_tag = re.match(r".*\{(.+?)\}.*", url)
    if date_tag:
        date_string = date_tag.group(1)

        dnow = datetime.now()

        year = dnow.strftime(date_string)

        if date_string in ("%Y", "%y") and yearoffset:
            try:
                date_subs = int(year)
                year = str(date_subs - yearoffset)
            except ValueError:
                pass
        resolved_url = re.sub(r"\{.+?\}", year, url)
        resolved_file = resolved_url.split("/")[-1]

        return resolved_file, resolved_url, year

    else:
        nfile = url.split("/")[-1]
        return nfile, url, False

def fetch_changedate(dfile_url):
    if dfile_url.startswith("http"):
        changedate = date_modified_http(dfile_url)

    elif dfile_url.startswith("ftp"):
        changedate = date_modified_ftp(dfile_url)

    return changedate

def generate_pbar(filesize):
    return ProgressBar(widgets=[FileTransferSpeed(), ' ',
                                Bar(marker=RotatingMarker()), ' ',
                                Percentage(), ' ', ETA()],
                       maxval=filesize).start()

if __name__ == "__main__":
    main()

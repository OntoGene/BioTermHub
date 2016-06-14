import requests
def download_file_http(url):
    filename = url.split('/')[-1]
    # http://stackoverflow.com/a/16696317
    r = requests.get(url, stream=True)
    r.raise_for_status() #throw exception on HTTP error codes
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()

def download_file_ftp(url):
        filename = url.split('/')[-1]
        connection = ftplib.FTP(dfile_pathlist[2], timeout=TIMEOUT)
        connection.login()
        ftpconn.retrbinary('RETR %s' % filename, open(filename, 'wb').write)
        ftpconn.quit()

def getdeps(force=False):
    from os.path import exists, getctime
    from time import time
    import tarfile
    from zipfile import ZipFile
    
    try:
        from wget import download
    except ImportError:
        print "Error: Module 'wget' needed to download ressources."
        
    # If update is forced, files don't exist or are older than a week, run wget
    filedict = {"acindex.txt": "ftp://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/docs/acindex.txt",
                "sec_ac.txt": "ftp://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/docs/sec_ac.txt",
                "uniprot_sprot.dat.gz":"ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz",
                #"uniprot_trembl.dat.gz":"ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_trembl.dat.gz"
                "BIOGRID-ALL-LATEST.tab2.zip":"http://thebiogrid.org/downloads/archives/Latest%20Release/BIOGRID-ALL-LATEST.tab2.zip",
                "BIOGRID-IDENTIFIERS-LATEST.tab.zip":"http://thebiogrid.org/downloads/archives/Latest%20Release/BIOGRID-IDENTIFIERS-LATEST.tab.zip"
                }
    for dfile in filedict:
        if force or not exists(dfile) or (time() - getctime(dfile) > 604800):
            print "Downloading %s ..." % dfile
            download(filedict[dfile])
            if dfile.endswith(".gz"):
                print "\nExtracting compressed tarball %s ..." % dfile
                tfile = tarfile.open(dfile, "r:gz")
                tfile.extractall()
                tfile.close()
            elif dfile.endswith(".zip"):
                print "\nExtracting zip file %s ..." %dfile
                zfile = ZipFile(dfile)
                zfile.extractall()
                zfile.close()

            print ""
    
    print "Download complete."

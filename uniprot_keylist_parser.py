class RecType(object):
    # Preliminary; some single-line items may be multi
    #~ single = ("ID", "AC", "SQ", "GN", "OC", "OS", "PE", "RA", "RL", "RN", "RP", "RX")
    #~ multi = ("DE", "CC", "OH", "DT", "KW", "RT", "FT", "DR")
    single = ("ID", "AC", "OS")
    multi = ()
    joinchar = "\n"

class Record(dict):
    """
    This record stores the information of one keyword or category in the
    keywlist.txt as a Python dictionary. The keys in this dictionary are
    the line codes that can appear in the keywlist.txt file:

    ---------  ---------------------------     ----------------------
    Line code  Content                         Occurrence in an entry
    ---------  ---------------------------     ----------------------
    ID         Identifier (keyword)            Once; starts a keyword entry.
    AC         Accession (KW-xxxx)             Once.
    DE         Definition                      Once or more.
    CC         Subcellular Location            Once or more; comments.
    SQ         Sequence                        Once; contains only the heading information.
    """
    def __init__(self):
        dict.__init__(self)
        for keyword in RecType.multi:
            self[keyword] = []
    
def parse(handle): # The parameter handle is the UniProt KeyList file.
    record = Record()
    # Now parse the records
    for line in handle:
        key = line[:2]
        if key=="//": # The last line of the current record has been reached.
            for rectype in RecType.multi:
                if len(record[rectype]) > 1:
                    record[rectype] = RecType.joinchar.join(record[rectype])
                elif len(record[rectype]) == 1:
                    record[rectype] = record[rectype][0]
                else:
                    record[rectype] = ""
            yield record     # So we output the record and pass to other one. 
            record = Record()
        elif line[2:5]=="   ": # If not, we continue recruiting the information. 
            value = line[5:].strip()
            if key in RecType.single:
                if key == "ID":
                    record[key] = value.split()[0]
                else:
                    record[key] = value.rstrip(";").rstrip(".")
            elif key in RecType.multi:
                record[key].append(value) 
            else:
                pass
            
    # Read the footer and throw it away
    for line in handle:
        pass

def getdeps(force=False):
    from os.path import exists, getctime
    from time import time
    import tarfile
    
    try:
        from wget import download
    except ImportError:
        print "Error: Module 'wget' needed to download ressources."
        
    # If update is forced, files don't exist or are older than a week, run wget
    filedict = {"acindex.txt": "ftp://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/docs/acindex.txt",
                "sec_ac.txt": "ftp://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/docs/sec_ac.txt",
                "uniprot_sprot.dat.gz":"ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz"
                #"uniprot_trembl.dat.gz":"ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_trembl.dat.gz"
                }
    for dfile in filedict:
        if force or not exists(dfile) or (time() - getctime(dfile) > 604800):
            print "Downloading %s ..." % dfile
            download(filedict[dfile])
            if dfile.endswith(".gz"):
                print "\nExtracting %s ..." % dfile
                tfile = tarfile.open(dfile, "r:gz")
                tfile.extractall()
            print ""
    
    print "Download complete."
        
    
def build_dictionary(source):
    map_dict = {}
    continuation = False
    continued_record = None
    
    for line in source:
        if "  " in line:
            line = line.split("  ")
            line[-1] = line[-1].rstrip("\n")
            if (len(line) == 1 and len(line[0])) or len(line) > 1:
                if line[0]:
                    map_dict[line[0]] = line[-1]
                elif continuation:
                    map_dict[continued_record] = map_dict[continued_record] + " " + line[-1]
                    if line[-1][-1] != ",":
                        continuation = False
                        continued_record = None
                    
                if line[-1][-1] == "," and not continuation:
                    continuation = True
                    continued_record = line[0]
            else:
                break
        
    #~ for element in map_dict.values():
        #~ if element[-1] == ",":
            #~ print element
            #~ raw_input()
    
    return map_dict

# Adapted from http://mannheimiagoesprogramming.blogspot.ch/2012/04/uniprot-keylist-file-parser-in-python.html

from copy import deepcopy

# Constants
SINGLE_RECORD = 1
MULTI_RECORD = 2

class RecType(object):
    # Preliminary; some single-line items may be multi
    #~ single = ("ID", "AC", "SQ", "GN", "OC", "OS", "PE", "RA", "RL", "RN", "RP", "RX")
    #~ multi = ("DE", "CC", "OH", "DT", "KW", "RT", "FT", "DR")
    single = ("ID", "AC")
    multi = ("OS", )
    joinchar = " "

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
    
class RecordSet(object):
    def __init__(self, infile):
        self.handle = open(infile, "r")
        self.record_set = {record["ID"]:record for record in self.parse(self.handle)}
        
    def parse(self, handle): # The parameter handle is the UniProt KeyList file.
        record = Record()
        mode = SINGLE_RECORD
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
                if mode == MULTI_RECORD:
                    record_list = [record]
                    for element in range(len(value_list) - 1):
                        additional_record = deepcopy(record)
                        record_list.append(additional_record)  
                    
                    record_value_pairs = zip(record_list, value_list)
                    for record, value in record_value_pairs:
                        record["AC"] = value
                        yield record
                else:
                    yield record     # So we output the record and pass to other one. 
                record = Record()
                mode = SINGLE_RECORD 
            elif line[2:5]=="   ": # If not, we continue recruiting the information. 
                value = line[5:].strip()
                if key in RecType.single:
                    if key == "ID":
                        value = value.split()[0]
                    elif key == "AC":
                        value_list = value.split()
                        value_list_len = len(value_list)
                        if len(value_list) > 1:
                            mode = MULTI_RECORD
                    if mode == SINGLE_RECORD or key != "AC":
                        value = value.rstrip(";").rstrip(".")
                        record[key] = value
                    elif mode == MULTI_RECORD and key == "AC":
                        for idx, value in enumerate(value_list):
                            value_list[idx] = value.rstrip(";").rstrip(".")

                elif key in RecType.multi:
                    record[key].append(value) 
                else:
                    pass
                
        # Read the footer and throw it away
        for line in handle:
            pass
            
    def build_dictionary(self, source):
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

    def build_indices(self, sec_ac, acindex):
        with open(sec_ac, "r") as sec_ac_obj:
            sec_ac_dict = build_dictionary(sec_ac_obj)
        with open(acindex, "r") as acindex_obj:
            acindex_dict = build_dictionary(acindex_obj)
        return sec_ac_dict, acindex_dict

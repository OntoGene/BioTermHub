# Adapted from http://mannheimiagoesprogramming.blogspot.ch/2012/04/uniprot-keylist-file-parser-in-python.html

from copy import deepcopy
from oid_generator import OID

class RecType(object):
    """
    Container class containing constants and constant tuples
    """
    single = ("ID", "AC")
    multi = ("OS", )
    
    og_mapping = {"AC":"original_id", "OS":"term"}
    
    SINGLE_RECORD = 1
    MULTI_RECORD = 2
    
    JOINCHAR = " "

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
    """
    def __init__(self, ontogene = True):
        dict.__init__(self)
        for keyword in RecType.multi:
            key = mapping(keyword, ontogene)
            self[key] = []
    
class RecordSet(dict):
    """
    Parses UniProt KeyList records into dictionary-like Record objects and stores these Records
    in a dictionary with the key-value structure record["ID"]:record.
    """
    def __init__(self, infile, ontogene = True):
        dict.__init__(self)
        self.handle = open(infile, "r")
        self.build_dict(ontogene)
        
    def build_dict(self, ontogene):
        for record in self.parse(self.handle, ontogene):
            if ontogene:
                key = mapping("AC", ontogene)
                self[record[key]] = record
            else:
                self[record["ID"]] = record
        
    def parse(self, handle, ontogene): # The parameter handle is the UniProt KeyList file.
        record = Record(ontogene)
        mode = RecType.SINGLE_RECORD
            
        # Now parse the records
        for line in handle:
            key = line[:2]
            
            if key=="//": # The last line of the current record has been reached.
                for rectype in RecType.multi:
                    reckey = mapping(rectype, ontogene)
                    if len(record[reckey]) > 1:
                        record[reckey] = RecType.JOINCHAR.join(record[reckey])
                    elif len(record[reckey]) == 1:
                        record[reckey] = record[reckey][0]
                    else:
                        record[reckey] = ""
                        
                    if reckey == "term":
                        record["preferred_term"] = record[reckey]
                        
                if mode == RecType.MULTI_RECORD:
                    record_list = [record]
                    for element in range(len(value_list) - 1):
                        additional_record = deepcopy(record)
                        record_list.append(additional_record)  
                    record_value_pairs = zip(record_list, value_list)
                    for record, value in record_value_pairs:
                        ac_key = mapping("AC", ontogene)
                        record[ac_key] = value
                        
                        yield record
                else:
                    yield record     # So we output the record and pass to other one. 
                record = Record(ontogene)
                mode = RecType.SINGLE_RECORD
            
            elif line[2:5]=="   ": # If not, we continue recruiting the information. 
                value = line[5:].strip()
                
                if not key == "ID":
                    mkey = mapping(key, ontogene)
                else:
                    mkey = None
                    
                if key in RecType.single:
                    if key == "ID" and not ontogene:
                        value = value.split()[0]
                        value = value.rstrip(";").rstrip(".")
                        record[key] = value
                        
                    elif key == "AC":
                        value_list = value.split()
                        value_list_len = len(value_list)
                        if len(value_list) > 1:
                            mode = RecType.MULTI_RECORD
                        # Generate OID
                        if ontogene:
                            record["oid"] = OID.get()
                            record["resource"] = "Uniprot"
                            record["entity_type"] = "protein"
                        
                    if key != "ID" and (mode == RecType.SINGLE_RECORD or key != "AC"):
                        value = value.rstrip(";").rstrip(".")
                        record[mkey] = value
                        if ontogene and key == "OS":
                            record["preferred_term"] = value
                        
                    elif mode == RecType.MULTI_RECORD and key == "AC":
                        for idx, value in enumerate(value_list):
                            value_list[idx] = value.rstrip(";").rstrip(".")
                            
                elif key in RecType.multi:
                    record[mkey].append(value) 
                
                else:
                    pass
                
        # Read the footer and throw it away
        for line in handle:
            pass
            
    def get_rowlist(self):
        return self.values()

def mapping(rectype, ontogene):
        if ontogene and rectype in RecType.og_mapping:
            return RecType.og_mapping[rectype]
        else:
            return rectype

# Presently unused
#

class Indices(object):
    """
    Generates and stores dictionaries for the Uniprot indices sec_ac and acindex.
    """
    def __init__(self, sec_ac, acindex):
        self.sec_ac = self.build_index(sec_ac)
        self.acindex = self.build_index(acindex)

    def build_index(self, index_file):
        with open(index_file, "r") as index_obj:
            index_dict = self.build_index_dictionary(index_obj)
        
        return index_dict
            
    def build_index_dictionary(self, source):
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
                    
        return map_dict

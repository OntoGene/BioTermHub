# Adapted from http://mannheimiagoesprogramming.blogspot.ch/2012/04/uniprot-keylist-file-parser-in-python.html
# Extended with cellosaurus functionality

from copy import deepcopy
from numpy import prod
from oid_generator import OID


SINGLE_RECORD = 1
MULTI_RECORD = 2

JOINCHAR = " "


class UniProtRecTypes(object):
    """
    Container class containing constants and constant tuples
    """
    resource = "Uniprot"
    entity_type = "protein"
    
    identifier = "AC"
    
    single = ("ID", "AC")
    multi_row = ("OS", )
    multi_value = "AC"
    
    og_mapping = {"AC":"original_id", "OS":"term"}
    
class CellosaurusRecTypes(object):
    """
    Container class containing constants and constant tuples
    """
    resource = "Cellosaurus"
    entity_type = "undefined" #TODO
    
    identifier = "SY"
    
    single = ("ID", "AC", "SY")
    multi_row = ()
    multi_value = "SY"
    
    og_mapping = {"AC":"original_id", "SY":"term"}


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
    def __init__(self, rectypes, ontogene = True):
        dict.__init__(self)
        for keyword in rectypes.multi_row:
            key = mapping(keyword, rectypes, ontogene)
            self[key] = []
    
class RecordSet(dict):
    """
    Parses UniProt KeyList records into dictionary-like Record objects and stores these Records
    in a dictionary with the key-value structure record["ID"]:record.
    """
    def __init__(self, infile, rectypes, ontogene = True):
        dict.__init__(self)
        self.handle = open(infile, "r")
        self.rectypes = rectypes
        self.build_dict(ontogene)
        
    def build_dict(self, ontogene):
        for record in self.parse(self.handle, ontogene):
            key = mapping(self.rectypes.identifier, self.rectypes, ontogene)
            self[record[key]] = record
        
    def parse(self, handle, ontogene): # The parameter handle is the UniProt KeyList file.
        record = Record(self.rectypes, ontogene)
        mode = SINGLE_RECORD
        multi_value_list = []
        
        # Now parse the records
        for line in handle:
            key = line[:2]
            
            if key=="//": # The last line of the current record has been reached.
                for m_rectype in self.rectypes.multi_row:
                    reckey = mapping(m_rectype, self.rectypes, ontogene)
                    if len(record[reckey]) > 1:
                        record[reckey] = JOINCHAR.join(record[reckey])
                    elif len(record[reckey]) == 1:
                        record[reckey] = record[reckey][0]
                    else:
                        record[reckey] = ""
                        
                    if reckey == "term":
                        record["preferred_term"] = record[reckey]
                        
                if mode == MULTI_RECORD:
                    record_list = [record]
                
                    # copy record approriate times
                    for element in range(len(multi_value_list) - 1):
                        additional_record = deepcopy(record)
                        record_list.append(additional_record)  
                    
                    record_value_pairs = zip(record_list, multi_value_list)
                    for record, value in record_value_pairs:
                        mapped_multival_key = mapping(self.rectypes.multi_value, self.rectypes, ontogene)
                        print mapped_multival_key
                        record[mapped_multival_key] = value
                        yield record
                else:
                    yield record     # So we output the record and pass to other one. 
                record = Record(self.rectypes, ontogene)
                mode = SINGLE_RECORD
                multi_value_list = []
            
            elif line[2:5]=="   ": # If not, we continue recruiting the information. 
                value = line[5:].strip()
                
                if not key == "ID":
                    mkey = mapping(key, self.rectypes, ontogene)
                else:
                    mkey = None
                    
                if key in self.rectypes.single:
                    if key == "ID" and not ontogene:
                        value = value.split()[0]
                        value = value.rstrip(";").rstrip(".")
                        record[key] = value
                        
                    elif key == self.rectypes.multi_value:
                        value_list = value.rstrip(";").split(";")
                        if len(value_list) > 1:
                            mode = MULTI_RECORD
                        
                        # Generate OID
                        if ontogene:
                            record["oid"] = OID.get()
                            record["resource"] = self.rectypes.resource
                            record["entity_type"] = self.rectypes.entity_type
                        
                    if key != "ID" and (mode == SINGLE_RECORD or key != self.rectypes.multi_value):
                        value = value.rstrip(";").rstrip(".")
                        record[mkey] = value
                        if ontogene and key == "OS":
                            record["preferred_term"] = value
                        
                    elif mode == MULTI_RECORD and key == self.rectypes.multi_value:
                        for idx, value in enumerate(value_list):
                            multi_value_list.append(value.strip().rstrip("."))
                            
                elif key in self.rectypes.multi_row:
                    record[mkey].append(value) 
                
                else:
                    pass
                
        # Read the footer and throw it away
        for line in handle:
            pass
            
    def get_rowlist(self):
        return self.values()

def mapping(rectype, rectypes, ontogene):
        if ontogene and rectype in rectypes.og_mapping:
            return rectypes.og_mapping[rectype]
        else:
            return rectype

# Presently unused
#

class UniProtIndices(object):
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

# Adapted from http://mannheimiagoesprogramming.blogspot.ch/2012/04/uniprot-keylist-file-parser-in-python.html
# Extended with cellosaurus functionality

from collections import defaultdict
from oid_generator import OID
from tools import DefaultOrderedDict, StatDict

# Property concatenation
SINGLE_RECORD = 1
MULTI_RECORD = 2

# Property split
MVAL_PERMUTATION = (1, 0)
MVAL_ADDITION = (0, 1)

JOINCHAR = " "


class UniProtRecTypes(object):
    """
    Container class containing constants and constant tuples
    """
    resource = "Uniprot"
    entity_type = "protein"
    
    accession = "AC"
    
    identifier = "ID"
    
    main_term = "OS"
    
    single = ("ID", "AC")
    multi_row = ("OS", )
    multi_value = "AC"
    synonym = None
    
    mode = "permutation"
    mv_neg_offset = MVAL_PERMUTATION[0]
    mv_pos_offset = MVAL_PERMUTATION[1]
    
    og_mapping = {"AC":"original_id", "ID":"original_id", "OS":"term"}

    rec_unit = "terms"
    ambig_unit = "ids"

    
class CellosaurusRecTypes(object):
    """
    Container class containing constants and constant tuples
    """
    resource = "Cellosaurus"
    entity_type = "cell_line"
    
    identifier = None
    
    accession = "AC"
    
    main_term = "ID"
    
    single = ("ID", "AC", "SY")
    multi_row = ()
    multi_value = "SY"

    synonym = "SY"
    
    mode = "addition"
    mv_neg_offset = MVAL_ADDITION[0]
    mv_pos_offset = MVAL_ADDITION[1]
    
    og_mapping = {"AC":"original_id", "ID":"term", "SY":"term"}

    rec_unit = "ids"
    ambig_unit = "terms"

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
    
class RecordSet(object):
    """
    Parses UniProt KeyList records into dictionary-like Record objects and stores these Records
    in a dictionary with the key-value structure record["ID"]:record.
    """
    def __init__(self, infile, rectypes, rowdicts = True, ontogene = True, all_acs = False, include_ids = True):
        self.infile = open(infile, "r")
        self.rectypes = rectypes
        self.ontogene = ontogene
        self.parsedict = {}
        self.stats = StatDict()
        self.ambiguous_count = 0
        self.ambig_unit = rectypes.ambig_unit
        self.all_acs = all_acs if not include_ids else False
        self.include_ids = include_ids if rectypes.identifier else False
        if not rowdicts:
            self.build_dict(ontogene)
    
    def build_dict(self, ontogene):
        self.record_gen = self.parse(self.handle, ontogene)
        for record in self.record_gen:
            key = mapping(self.rectypes.accession, self.rectypes, ontogene)
            self.parsedict[record[key]] = record
    @property
    def rowdicts(self): # The parameter handle is the UniProt KeyList file.
        ontogene = self.ontogene
        record = Record(self.rectypes, ontogene)
        mode = SINGLE_RECORD
        ac, id = None, None
        multi_value_list = []
        
        # Now parse the records
        for line in self.infile:
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

                if self.include_ids:
                    id_record = record.copy()
                    ac_key = mapping("AC", self.rectypes, ontogene)
                    id_key = mapping("ID", self.rectypes, ontogene)
                    record["resource"] = self.rectypes.resource + " (AC)"
                    id_record["resource"] = self.rectypes.resource + " (ID)"
                    record[ac_key] = ac
                    id_record[id_key] = id
                    yield id_record
                    yield record

                elif mode == MULTI_RECORD:
                    record_list = [record]
                
                    # copy record. Permutations: copy record len(multi_value_list) - 1, Additions: copy record len(multi_value_list)
                    rec_range = range(len(multi_value_list) - self.rectypes.mv_neg_offset)
                    for element in rec_range:
                        additional_record = record.copy()
                        record_list.append(additional_record)  
                     
                    # In case of additions, yield first record before zipping and yielding the remaining records
                    if self.rectypes.mv_pos_offset:
                        self.stats[self.rectypes.ambig_unit] += 1
                        yield record
                    
                    # Offset if the multiple records are additions (not just permutations)
                    record_value_pairs = zip(record_list[self.rectypes.mv_pos_offset:], multi_value_list)
                    
                    # Zip record list with value list for multi-value entry, yield records iteratively
                    for record, value in record_value_pairs:
                        mapped_multival_key = mapping(self.rectypes.multi_value, self.rectypes, ontogene)
                        record[mapped_multival_key] = value
                        self.stats[self.rectypes.ambig_unit] += 1
                        yield record
                else:
                    self.stats[self.rectypes.ambig_unit] += 1
                    yield record     # So we output the record and pass to other one. 
                record = Record(self.rectypes, ontogene)
                mode = SINGLE_RECORD
                multi_value_list = []
                ac, id = None, None
            
            elif line[2:5]=="   ": # If not, we continue recruiting the information. 
                value = line[5:].strip()
                
                mkey = mapping(key, self.rectypes, ontogene)
                    
                if key in self.rectypes.single:
                    
                    if key == self.rectypes.identifier and self.include_ids:
                        value = value.split()[0]
                        value = value.rstrip(";").rstrip(".")
                        id = value
                        record["oid"] = OID.get()
                        record["entity_type"] = self.rectypes.entity_type
                        self.stats[self.rectypes.rec_unit] += 1
                    
                    elif key == self.rectypes.accession:
                        # Generate OID
                        record["oid"] = OID.get() if not self.include_ids else OID.last()
                        record["entity_type"] = self.rectypes.entity_type
                        if not self.include_ids:
                            record["resource"] = self.rectypes.resource
                            self.stats[self.rectypes.rec_unit] += 1

                        
                    if key == self.rectypes.multi_value:
                        value_list = value.rstrip(";").split(";")
                        if key == self.rectypes.accession or key == self.rectypes.synonym:
                            # ambiguous units per record
                            statkey_value = len(value_list)
                            statkey_string = "%s/%s" % (self.rectypes.ambig_unit, self.rectypes.rec_unit[:-1])
                            statkey = (statkey_value, statkey_string)
                            self.stats["ratios"][statkey] += 1
                        if self.rectypes.mode == "permutation" and len(value_list) > 1 \
                           or self.rectypes.mode == "addition":
                            if self.all_acs:
                                self.ambiguous_count += 1
                                mode = MULTI_RECORD
                            else:
                                if not self.include_ids:
                                    value = value_list[0]
                                else:
                                    ac = value_list[0]
                        else:
                            ac = value.rstrip(";").rstrip(".")
                            
                    if (mode == SINGLE_RECORD or key != self.rectypes.multi_value):
                        if key != self.rectypes.identifier or \
                                (self.include_ids and
                                    key != self.rectypes.identifier and key != self.rectypes.accession):

                            value = value.rstrip(";").rstrip(".")
                            record[mkey] = value
                            if ontogene and key == self.rectypes.main_term:
                                record["preferred_term"] = value
                        
                    elif mode == MULTI_RECORD and key == self.rectypes.multi_value:
                        for idx, value in enumerate(value_list):
                            multi_value_list.append(value.strip().rstrip("."))
                            
                elif key in self.rectypes.multi_row:
                    record[mkey].append(value) 
                    
                else:
                    pass
                
        # Read the footer and throw it away
        for line in self.infile:
            pass

        # print "Ambiguous count",  self.ambiguous_count

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

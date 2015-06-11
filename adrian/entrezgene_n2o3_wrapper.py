from optparse import OptionParser
from ncbi2ontogene3 import process_file, transform_input
from oid_generator import OID
from copy import deepcopy

class RecType(object):
    
    og_mapping = {'ncbi_id':"original_id", 'term':"term", 'reference':"preferred_term", 'type':"entity_type"}

class RecordSet(dict):
    """
    Uses the external parser ncbi2ontogene3 to process NCBI/EntrezGene records 
    into dictionaries and stores these dictionaries in a dictionary with the key-value structure 
    record[ncbi_id]:record.
    """
    
    def __init__(self, infile, ontogene = True):
        dict.__init__(self)
        self.rowlist = self._run_ncbi2ontogene3(infile, "default", ontogene)
        self = self.build_dict(infile, ontogene)
    
    def get_rowlist(self):
        return self.rowlist
    
    def build_dict(self, infile, ontogene):
        # Map the row dictionaries to a nested dictionary structure with ncbi_id as key 
        for rowdict in self.rowlist:
            
            rowkey, rowvalue_dict = int(rowdict[mapping('ncbi_id', ontogene)]), rowdict
            
            # Change keys strings with ontogene key strings
            if ontogene:
                rowvalue_dict["resource"] = "Entrezgene"
                
            if rowkey in self:
                rowvalue_dict["oid"] = OID.last()
                try:
                    # value is already a list
                    self[rowkey].append(rowvalue_dict)
                except AttributeError:
                    # encapsulate first rowvalue_dict in list, append second rowvalue_dict
                    rowvalue_list = [self[rowkey]]
                    rowvalue_list.append(rowvalue_dict)
                    self[rowkey] = rowvalue_list
            else:    
                rowvalue_dict["oid"] = OID.get()
                self[rowkey] = rowvalue_dict
                
    def _run_ncbi2ontogene3(self, infile, options, ontogene):
        if options == "default":
            options, args = OptionParser(option_list=["--id_list"]).parse_args()
        
        # Using ncbi2ontogene3 to retrieve a list with a dictionary for each row
        processed_input = process_file(infile, options, None)
        if ontogene:
            rowlist = transform_input(processed_input, RecType.og_mapping)
        else:
            rowlist = transform_input(processed_input)
            
        return rowlist

def mapping(rectype, ontogene):
        if ontogene and rectype in RecType.og_mapping:
            return RecType.og_mapping[rectype]
        else:
            return rectype

#def keydict_pair(indict):
#    key = int(indict["ncbi_id"])
#    value_dict = {k:v for k,v in indict.iteritems() if k != "ncbi_id"}
#    return key, value_dict

from optparse import OptionParser
from ncbi2ontogene3 import process_file, transform_input

class RecordSet(dict):
    """
    Uses the external parser ncbi2ontogene3 to process NCBI/EntrezGene records 
    into dictionaries and stores these dictionaries in a dictionary with the key-value structure 
    record[ncbi_id]:record.
    """
    
    def __init__(self, infile):
        dict.__init__(self)
        self = self.build_dict(infile)
    
    def build_dict(self, infile, options = "default"):
        if options == "default":
            options, args = OptionParser(option_list=["--id_list"]).parse_args()
        
        # Using ncbi2ontogene3 to retrieve a list with a dictionary for each row
        processed_input = process_file(infile, options, None)
        dictlist = transform_input(processed_input)
        
        # Map these dictionaries to a nested dictionary structure with ncbi_id as key 
        for rowdict in dictlist:
            rowkey, rowvalue_dict = keydict_pair(rowdict)
            if rowkey in self:
                try:
                    # value is already a list
                    self[rowkey].append(rowvalue_dict)
                except AttributeError:
                    # encapsulate first rowvalue_dict in list, append second rowvalue_dict
                    rowvalue_list = [self[rowkey]]
                    rowvalue_list.append(rowvalue_dict)
                    self[rowkey] = rowvalue_list
            else:    
                self[rowkey] = rowvalue_dict

def keydict_pair(indict):
    key = int(indict["ncbi_id"])
    value_dict = {k:v for k,v in indict.iteritems() if k != "ncbi_id"}
    return key, value_dict

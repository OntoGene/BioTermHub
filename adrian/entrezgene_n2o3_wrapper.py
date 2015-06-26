from optparse import OptionParser
from ncbi2ontogene3 import process_file, transform_input
from collections import Counter
import ncbi_preprocess

class RecType(object):
    
    og_mapping = {'ncbi_id':"original_id", 'term':"term", 'reference':"preferred_term", 'type':"entity_type"}

class RecordSet(object):
    """
    Uses the external parser ncbi2ontogene3 to process NCBI/EntrezGene records 
    into dictionaries and stores these dictionaries in a dictionary with the key-value structure 
    record[ncbi_id]:record.
    """
    
    def __init__(self, infile, ontogene = True):
        self.prepfile = infile+".trunc"
        ncbi_preprocess.preprocess(infile, self.prepfile, 1, 2, 4)
        self.options = "default"
        self.ontogene = ontogene
        self.stats = Counter({"ids":0, "terms":0})
        self.parsedict = {}
        
    def build_dict(self, ontogene):
        # Map the row dictionaries to a nested dictionary structure with ncbi_id as key 
        for rowdict in self.raw_rowlist:
            
            rowkey, rowvalue_dict = int(rowdict[mapping('ncbi_id', ontogene)]), rowdict
            
            # Change keys strings with ontogene key strings
                    
            if rowkey in self:
                try:
                    # value is already a list
                    self.parsedict[rowkey].append(rowvalue_dict)
                except AttributeError:
                    # encapsulate first rowvalue_dict in list, append second rowvalue_dict
                    rowvalue_list = [self.parsedict[rowkey]]
                    rowvalue_list.append(rowvalue_dict)
                    self.parsedict[rowkey] = rowvalue_list
            else: 
                self.parsedict[rowkey] = rowvalue_dict
                
    @property
    def rowdicts(self):
        if self.options == "default":
            options, args = OptionParser(option_list=["--id_list"]).parse_args()
        
        # Using ncbi2ontogene3 to retrieve a dictionary for each row
        processed_input = process_file(self.prepfile, options, None)

        for row, new_id in transform_input(processed_input, RecType.og_mapping, unified_build=True):
            self.stats["terms"] += 1
            if new_id:
                self.stats["ids"] += 1
            yield row

def mapping(rectype, ontogene):
        if ontogene and rectype in RecType.og_mapping:
            return RecType.og_mapping[rectype]
        else:
            return rectype

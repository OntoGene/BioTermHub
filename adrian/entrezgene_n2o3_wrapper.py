from optparse import OptionParser
from ncbi2ontogene3 import process_file, transform_input
from oid_generator import OID
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
    
    def __init__(self, infile, rowdicts = True, ontogene = True):
        self.prepfile = infile+".trunc"
        ncbi_preprocess.preprocess(infile, self.prepfile ,1,2,4)
        self.raw_rowlist = self._run_ncbi2ontogene3(self.prepfile, "default", ontogene)
        self.rowdicts = []
        self.parsedict = {}
        self.stats = Counter({"ids":0, "terms":0})
        if rowdicts:
            self.get_rowlist(ontogene)
        else:
            self.build_dict(ontogene)
            
    def get_rowlist(self, ontogene):
        # Map the row dictionaries to a nested dictionary structure with ncbi_id as key 
        previous_rowkey = None
        for rowdict in self.raw_rowlist:
            
            rowkey, rowvalue_dict = int(rowdict[mapping('ncbi_id', ontogene)]), rowdict
            self.stats["terms"] += 1
            
            # Change keys strings with ontogene key strings
            if ontogene:
                rowvalue_dict["resource"] = "Entrezgene"
                
            if rowkey == previous_rowkey:
                rowvalue_dict["oid"] = OID.last()
            else:    
                rowvalue_dict["oid"] = OID.get()
                self.stats["ids"] += 1
                previous_rowkey = rowkey
            
            self.rowdicts.append(rowvalue_dict)
    
    def build_dict(self, ontogene):
        # Map the row dictionaries to a nested dictionary structure with ncbi_id as key 
        for rowdict in self.raw_rowlist:
            
            rowkey, rowvalue_dict = int(rowdict[mapping('ncbi_id', ontogene)]), rowdict
            
            # Change keys strings with ontogene key strings
            if ontogene:
                rowvalue_dict["resource"] = "Entrezgene"
                
            if rowkey in self:
                rowvalue_dict["oid"] = OID.last()
                try:
                    # value is already a list
                    self.parsedict[rowkey].append(rowvalue_dict)
                except AttributeError:
                    # encapsulate first rowvalue_dict in list, append second rowvalue_dict
                    rowvalue_list = [self.parsedict[rowkey]]
                    rowvalue_list.append(rowvalue_dict)
                    self.parsedict[rowkey] = rowvalue_list
            else:    
                rowvalue_dict["oid"] = OID.get()
                self.parsedict[rowkey] = rowvalue_dict
                
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

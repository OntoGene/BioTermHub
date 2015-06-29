from collections import defaultdict
from mesh_desc_supp2extended_ontogene import parse_desc_file, parse_supp_file, desc2ontogene_headers, supp2ontogene_headers
from tools import DefaultOrderedDict, StatDict

class RecordSet(object):
    """
    Uses the external parser mesh_desc_supp2extended_ontogene to process MESH descriptor and 
    supplement records into dictionaries and stores these dictionaries in a dictionary with 
    the key-value structure record[ncbi_id]:record.
    """
    
    def __init__(self, desc_file, supp_file, trees = ["B","C","D"], rowdicts = True, ontogene = True):
        self.stats = None
        self.rowdicts = self._run_mesh_parser(desc_file, supp_file, ontogene)
        self.parsedict = {}
        self.stats = StatDict()
        self.ambig_unit = "terms"
        self.trees = list(trees)
        if not rowdicts:
            self.build_dict(ontogene)
    
    def get_rowlist(self, ontogene):
        return self.rowdicts
    
    def build_dict(self, ontogene):
        # Map the row dictionaries to a nested dictionary structure with ncbi_id as key 
        for rowdict in self.raw_rowlist:
            
            rowkey, rowvalue_dict = rowdict['original_id'], rowdict
            
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
                
    def _run_mesh_parser(self, desc_file, supp_file, ontogene):
        # Using mesh_desc_supp2extended_ontogene to retrieve a list with a dictionary for each row
        
        (desc_dict_list, desc_tree_dict) = parse_desc_file(desc_file)

        supp_dict_list = parse_supp_file(supp_file)
        
        relevant_trees = set(self.trees + ["empty_branch"])

        generators = (desc2ontogene_headers(relevant_trees, desc_dict_list),
                      supp2ontogene_headers(relevant_trees, supp_dict_list, desc_tree_dict))

        terms_per_id = 0
        for generator in generators:
            for row, new_record in generator:
                self.stats["terms"] += 1
                terms_per_id += 1

                if new_record:
                    self.stats["ids"] += 1
                    statkey = (terms_per_id, "terms/id")
                    self.stats["ratios"][statkey] += 1
                    terms_per_id = 1

                yield row
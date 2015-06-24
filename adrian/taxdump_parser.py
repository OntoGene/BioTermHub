from oid_generator import OID
from collections import Counter, defaultdict
import re

class RecordSet(object):
    def __init__(self, infile, ontogene = True):
        header_fields = ["tax_id", "name_txt", "unique_name", "name_class"]
        self.reader = (dict(zip(header_fields, line.rstrip("\t|\n").split("\t|\t"))) for line in open(infile, "r"))
        self.infile = infile
        self.ontogene = ontogene
        self.stats = Counter({"ids":0, "terms":0})
        
    @property
    def rowdicts(self):
        ontogene = self.ontogene
        previous_tax_id = None
        for rowvalue_dict in self.reader:
            tax_id = rowvalue_dict["tax_id"]
            # If a new tax_id is reached
            if tax_id and tax_id != previous_tax_id:
                if previous_tax_id:
                    self.stats["ids"] += 1
                    pref_term = record_dict["scientific name"][0]
                    oid = OID.get()
                    for category, value_list in record_dict.iteritems():
                        for value in value_list:
                            if category not in ["authority"]:
                                outvalue_dict = {}
                                outvalue_dict["original_id"] = previous_tax_id
                                outvalue_dict["term"] = value
                                outvalue_dict["preferred_term"] = pref_term
                                outvalue_dict["entity_type"] = "species"
                                if ontogene:
                                    outvalue_dict["resource"] = "NCBI Taxonomy"
                                    outvalue_dict["oid"] = oid
                                self.stats["terms"] += 1    
                                yield outvalue_dict
                        
                record_dict = defaultdict(list)
                
            name_class = rowvalue_dict["name_class"]
            name_txt = rowvalue_dict["name_txt"]
            if name_class == "synonym":
                match_quotes = re.match('\"(.*)\".*', name_txt)
                match_rparent = re.match('(.*?) \(.*', name_txt)
                if match_quotes:
                    name_txt = match_quotes.group(1)
                else:
                    if match_rparent:
                        name_txt = match_rparent.group(1)
                    
                    match_author_year = \
                        re.sub(r'((\S*(\sand\s\S*|\S*\set\sal.)?)\s\d{4,4})', '', name_txt)
                        
                    name_txt = match_author_year
                        
            record_dict[name_class].append(name_txt)
            
            previous_tax_id = tax_id

def mapping(rectype, ontogene):
    if ontogene and rectype in RecType.og_mapping:
        return RecType.og_mapping[rectype]
    else:
        return rectype

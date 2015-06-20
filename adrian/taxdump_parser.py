from oid_generator import OID



class RecordSet(object):
    def __init__(self, infile, ontogene = True):
        header_fields = ["tax_id", "name_txt", "unique_name", "name_class"]
        self.reader = (dict(zip(header_fields, line.rstrip("\t|\n").split("\t|\t"))) for line in open(infile, "r"))
        self.rowdicts = []
        self.get_rowlist(ontogene)
        
    def get_rowlist(self, ontogene):
        rowkeys = []
        for rowvalue_dict in self.reader:
            outvalue_dict = {}
            rowkey = rowvalue_dict["tax_id"]
            outvalue_dict["original_id"] = rowvalue_dict["tax_id"]
            outvalue_dict["term"] = rowvalue_dict["name_txt"]
            outvalue_dict["preferred_term"] = outvalue_dict["term"]
            outvalue_dict["entity_type"] = "species"
            if rowvalue_dict["name_class"] not in ("authority"):
                if ontogene:
                    outvalue_dict["resource"] = "NCBI Taxonomy"
                if rowkey in rowkeys:
                    outvalue_dict["oid"] = OID.last()
                else:    
                    outvalue_dict["oid"] = OID.get()
                    rowkeys.append(rowkey)
                
                self.rowdicts.append(outvalue_dict)

def mapping(rectype, ontogene):
        if ontogene and rectype in RecType.og_mapping:
            return RecType.og_mapping[rectype]
        else:
            return rectype

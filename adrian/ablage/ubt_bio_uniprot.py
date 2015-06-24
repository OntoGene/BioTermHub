from unified_builder import RecordSetContainer, UnifiedBuilder

test_rsc = RecordSetContainer("data/BIOGRID-IDENTIFIERS-3.4.125.tab.txt",
                              "data/uniprot_sprot.dat",
                              None,
                              None #"data/gene_info"
                              )

#~ test_rsc = RecordSetContainer("1k_snippets/BIOGRID-IDENTIFIERS-1k",
                              #~ "1k_snippets/uniprot_sprot-1k",
                              #~ "1k_snippets/cellosaurus-1k",
                              #~ "1k_snippets/gene_info_10k"
                              #~ )

biogrid_idents = test_rsc.biogrid_idents
uniprot_records = test_rsc.uniprot_records

counter_uni = 0

missing_UNIPROT = []

for ident in biogrid_idents:
    if "UNIPROT-ACCESSION" in biogrid_idents[ident]:
        counter_uni += 1
        if not biogrid_idents[ident]["UNIPROT-ACCESSION"] in uniprot_records:
            missing_UNIPROT.append(biogrid_idents[ident]["UNIPROT-ACCESSION"]+"\n")

print "Uniprot"
print len(missing_UNIPROT)
print counter_uni

import codecs

missing_u = codecs.open("missing_u", "w", "utf-8")
missing_u.writelines(missing_UNIPROT)
missing_u.close()

print "\nBIOGRID IDs"
print biogrid_idents.items()[0:5]
print "\nUniprot Records"
print uniprot_records.items()[0:5]
print "Processing ..."

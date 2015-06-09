from unified_builder import RecordSetContainer

test_rsc = RecordSetContainer("1k_snippets/BIOGRID-IDENTIFIERS-1k",
                              "1k_snippets/BIOGRID-ALL-1k",
                              "1k_snippets/uniprot_sprot-1k",
                              "1k_snippets/gene_info_10k")

print "BIOGRID IDs"
print test_rsc.biogrid_idents.items()[0:5]
print "\nBIOGRID Records"
print test_rsc.biogrid_records.items()[0:5]
print "\nUniprot Records"
print test_rsc.uniprot_records.items()[0:5]
print "\nEntrezgene Records"
print test_rsc.entrezgene_records.items()[0:5]

from unified_builder import RecordSetContainer, UnifiedBuilder

test_rsc = RecordSetContainer("1k_snippets/BIOGRID-IDENTIFIERS-1k",
                              #"1k_snippets/BIOGRID-ALL-1k",
                              "1k_snippets/uniprot_sprot-1k",
                              "1k_snippets/gene_info_10k")

biogrid_idents = test_rsc.biogrid_idents.items()
#biogrid_records = test_rsc.biogrid_records.items()
uniprot_records = test_rsc.uniprot_records.items()
entrezgene_records = test_rsc.entrezgene_records.items()

missing_ENTREZ_GENE = []
missing_UNIPROT = []

for ident in biogrid_idents:
    if "ENTREZ_GENE" in ident:
        if not ident["ENTREZ_GENE"] in entrezgene_records:
            missing_ENTREZ_GENE.append(ident["ENTREZ_GENE"])

    if "UNIPROT-ACCESSION" in ident:
        if not ident["UNIPROT"] in uniprot_records:
            missing_ENTREZ.append(ident["ENTREZ_GENE"])
            
print len(missing_ENTREZ_GENE)
print len(missing_UNIPROT)

#~ print "\nBIOGRID Records"
#~ print biogrid_records[0:5]
#~ print "..."
#~ print biogrid_records[-1]
print "\nUniprot Records"
print uniprot_records[0:5]
print "..."
print uniprot_records[-1]
print "\nEntrezgene Records"
print entrezgene_records[0:5]
print "..."
print entrezgene_records[-1]
print "\nBIOGRID IDs"
print biogrid_idents[0:5]
print "..."
print biogrid_idents [-1]


print "Processing ..."

UnifiedBuilder(test_rsc, "output.csv")

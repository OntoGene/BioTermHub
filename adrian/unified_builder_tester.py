from unified_builder import RecordSetContainer, UnifiedBuilder

#~ test_rsc = RecordSetContainer("data/BIOGRID-IDENTIFIERS-3.4.125.tab.txt",
                              #~ "data/uniprot_sprot.dat",
                              #~ None #"data/gene_info"
                              #~ )

test_rsc = RecordSetContainer("1k_snippets/BIOGRID-IDENTIFIERS-1k",
                              #"1k_snippets/BIOGRID-ALL-1k",
                              None, #"1k_snippets/uniprot_sprot-1k",
                              None #"1k_snippets/gene_info_10k"
                              )

biogrid_idents = test_rsc.biogrid_idents
#biogrid_records = test_rsc.biogrid_records
uniprot_records = test_rsc.uniprot_records
#entrezgene_records = test_rsc.entrezgene_records

counter_ent = 0
counter_uni = 0

missing_ENTREZ_GENE = []
missing_UNIPROT = []

for ident in biogrid_idents:
    #~ if "ENTREZ_GENE" in biogrid_idents[ident]:
        #~ counter_ent += 1
        #~ if not biogrid_idents[ident]["ENTREZ_GENE"] in entrezgene_records:
            #~ missing_ENTREZ_GENE.append(biogrid_idents[ident]["ENTREZ_GENE"])

    if "UNIPROT-ACCESSION" in biogrid_idents[ident]:
        counter_uni += 1
        if not biogrid_idents[ident]["UNIPROT-ACCESSION"] in uniprot_records:
            missing_UNIPROT.append(biogrid_idents[ident]["UNIPROT-ACCESSION"])

print len(missing_ENTREZ_GENE)
print counter_ent
print len(missing_UNIPROT)
print counter_uni

import codecs

missing_e = codecs.open("missing_e", "w", "utf-8")
missing_u = codecs.open("missing_u", "w", "utf-8")

missing_e.writelines(missing_ENTREZ_GENE)
missing_u.writelines(missing_UNIPROT)

missing_e.close()
missing_u.close()

#~ print "\nBIOGRID Records"
#~ print biogrid_records[0:5]
#~ print "..."
#~ print biogrid_records[-1]
#~ print "\nUniprot Records"
#~ print uniprot_records[0:5]
#~ print "..."
#~ print uniprot_records[-1]
#~ print "\nEntrezgene Records"
#~ print entrezgene_records[0:5]
#~ print "..."
#~ print entrezgene_records[-1]
#~ print "\nBIOGRID IDs"
#~ print biogrid_idents[0:5]
#~ print "..."
#~ print biogrid_idents [-1]

print "Processing ..."

UnifiedBuilder(test_rsc, "output.csv")

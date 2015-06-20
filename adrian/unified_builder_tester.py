from unified_builder import RecordSetContainer, UnifiedBuilder

#~ test_rsc = RecordSetContainer("data/uniprot_sprot.dat",
                              #~ "data/cellosaurus.txt",
                              #~ "data/gene_info",
                              #~ ("data/desc2015.xml", "data/supp2015.xml")
                              #~ )

test_rsc = RecordSetContainer(#"1k_snippets/BIOGRID-IDENTIFIERS-1k",
                              #"1k_snippets/BIOGRID-ALL-1k",
                              "1k_snippets/uniprot_sprot-1k",
                              "1k_snippets/cellosaurus-2",
                              "1k_snippets/gene_info_10k",
                              ("1k_snippets/desc-1k", "1k_snippets/supp-1k")
                              )

#biogrid_idents = test_rsc.biogrid_idents
#biogrid_records = test_rsc.biogrid_records
uniprot_records = test_rsc.uniprot_records
cellosaurus_records = test_rsc.cellosaurus_records
mesh_records = test_rsc.mesh_records
entrezgene_records = test_rsc.entrezgene_records

#~ counter_ent = 0
#~ counter_uni = 0
#~ 
#~ missing_ENTREZ_GENE = []
#~ missing_UNIPROT = []

#~ for ident in biogrid_idents:
    #~ if "ENTREZ_GENE" in biogrid_idents[ident]:
        #~ counter_ent += 1
        #~ if not biogrid_idents[ident]["ENTREZ_GENE"] in entrezgene_records:
            #~ missing_ENTREZ_GENE.append(biogrid_idents[ident]["ENTREZ_GENE"])

    #~ if "UNIPROT-ACCESSION" in biogrid_idents[ident]:
        #~ counter_uni += 1
        #~ if not biogrid_idents[ident]["UNIPROT-ACCESSION"] in uniprot_records:
            #~ missing_UNIPROT.append(biogrid_idents[ident]["UNIPROT-ACCESSION"]+"\n")

#~ print "Entrez gene"
#~ print len(missing_ENTREZ_GENE)
#~ print counter_ent
#~ print "Uniprot"
#~ print len(missing_UNIPROT)
#~ print counter_uni

#~ import codecs
#~ 
#~ missing_e = codecs.open("missing_e", "w", "utf-8")
#~ missing_u = codecs.open("missing_u", "w", "utf-8")
#~ 
#~ missing_e.writelines(missing_ENTREZ_GENE)
#~ missing_u.writelines(missing_UNIPROT)
#~ 
#~ missing_e.close()
#~ missing_u.close()

#~ print "\nBIOGRID Records"
#~ print biogrid_records[0:5]
#~ print "..."
#~ print biogrid_records[-1]
#~ print "\nBIOGRID IDs"
#print biogrid_idents.items()[0:5]
print "\nUniprot Records"
print uniprot_records.rowdicts[0:5]
print "..."
print uniprot_records.rowdicts[-1]
print "\nCellosaurus Records"
print cellosaurus_records.rowdicts[0:5]
print "..."
print cellosaurus_records.rowdicts[-1]
print "\nMESH Records"
print mesh_records.rowdicts[0:5]
print "..."
print mesh_records.rowdicts[-1]
print "\nEntrezgene Records"
print entrezgene_records.rowdicts[0:5]
print "..."
print entrezgene_records.rowdicts[-1]

print "Processing ..."

UnifiedBuilder(test_rsc, "output.csv")

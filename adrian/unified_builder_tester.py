from unified_builder import RecordSetContainer, UnifiedBuilder
import statplot
import statoutput
#~ from guppy import hpy
#~ h = hpy()
#~ h.setref()

# test_rsc = RecordSetContainer(uniprot = "../data/uniprot_sprot.dat")
                              #~ "data/cellosaurus.txt",
                              #~ "data/gene_info",
                              #~ ("data/desc2015.xml", "data/supp2015.xml")
                              #~ )

test_rsc = RecordSetContainer(
                              uniprot = "1k_snippets/uniprot_sprot-3",
                              cellosaurus = "1k_snippets/cellosaurus-2",
                              #entrezgene = "1k_snippets/gene_info-3",
                              mesh = ("1k_snippets/desc-1k", "1k_snippets/supp-1k"),
                              taxdump = "1k_snippets/names-30",
                              chebi = "1k_snippets/chebi-1k"
                              )

#biogrid_idents = test_rsc.biogrid_idents
#biogrid_records = test_rsc.biogrid_records
#~ uniprot_records = test_rsc.uniprot_records
#~ cellosaurus_records = test_rsc.cellosaurus_records
#~ mesh_records = test_rsc.mesh_records
#~ entrezgene_records = test_rsc.entrezgene_records
#~ taxdump_records = test_rsc.taxdump_records

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
#~ print "\nUniprot Records"
#~ print uniprot_records.rowdicts[0:5]
#~ print "..."
#~ print uniprot_records.rowdicts[-1]
#~ print "\nCellosaurus Records"
#~ print cellosaurus_records.rowdicts[0:5]
#~ print "..."
#~ print cellosaurus_records.rowdicts[-1]
#~ print "\nMESH Records"
#~ print mesh_records.rowdicts[0:5]
#~ print "...
#~ print mesh_records.rowdicts[-1]
#~ print "\nEntrezgene Records"
#~ print entrezgene_records.rowdicts[0:5]
#~ print "..."
#~ print entrezgene_records.rowdicts[-1]
#~ print "\nTaxdump Records"
#~ print taxdump_records.rowdicts[0:5]
#~ print "..."
#~ print taxdump_records.rowdicts[-1]

print "Processing ..."

a = UnifiedBuilder(test_rsc, "output.csv", True, True)

test_rsc.calcstats()
print test_rsc.stats

statoutput.write2tsv(test_rsc)
statplot.plotstats(test_rsc)



#~ a_normal = test_rsc.bidict_originalid_oid.items()
#~ a_inverse = test_rsc.bidict_originalid_oid.items(inverse = True)
#~ print a_normal[:10]
#~ print a_inverse[:10]
#~ 

#print test_rsc.bidict_originalid_term
#~ b_normal = test_rsc.bidict_originalid_term.items(to_list = True)
#~ b_inverse = test_rsc.bidict_originalid_term.items(inverse = True, to_list = True)
#~ print b_normal[:10]
#~ print b_inverse[:10]

#~ print h.heap()

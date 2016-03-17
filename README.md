# README #

### Used data
(Draft Adrian)

* [Uniprot Reviewed (Swiss-Prot) (text)](ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/completw/uniprot_sprot.dat.gz) (http://www.uniprot.org/downloads)
    * Additional files
        * ftp://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/docs/acindex.txt contains a mapping from accession number to uniprot mnemonic name.

        * ftp://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/docs/sec_ac.txt contains a mapping from secondary to primary accession numbers
     

* [BIOGRID-ALL-LATEST.tab2.zip](http://thebiogrid.org/downloads/archives/Latest%20Release/BIOGRID-ALL-LATEST.tab2.zip) (http://thebiogrid.org/download.php)

* [BIOGRID-IDENTIFIERS-LATEST.tab.zip](http://thebiogrid.org/downloads/archives/Latest%20Release/BIOGRID-IDENTIFIERS-LATEST.tab.zip) (http://thebiogrid.org/download.php)


### Tilia: ncbi2ontogene_commented.py

* Used data: [gene_info] (ftp://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz)
 
* TSV Format (Format: tax_id GeneID Symbol LocusTag Synonyms dbXrefs chromosome map_location description type_of_gene Symbol_from_nomenclature_authority Full_name_from_nomenclature_authority Nomenclature_status Other_designations Modification_date (tab is used as a separator, pound sign - start of a comment

* Preprocessed with: `C_ALL=C cut -f2,3,5,7,9,10,14 gene_info_changed > ncbi_gene_info_changed_relevant_cols`

WWWDIRS := $(shell python3 -m bth settings -dvr)
LOGFILE := $(shell python3 -m bth settings -vr -n log_file)
UMLSDIR := $(shell python3 -m bth settings -vr -n path_umls_maps)

init: $(WWWDIRS) $(LOGFILE)

$(WWWDIRS):
	mkdir -p $@ && chmod a+w,g+s $@

$(LOGFILE):
	touch $@ && chmod a+w $@


umls-update: $(UMLSDIR)/curl-uts-download.sh $(UMLSDIR)/uts.nlm.nih.gov.crt

$(UMLSDIR)/curl-uts-download.sh $(UMLSDIR)/uts.nlm.nih.gov.crt: terminology_download_script.zip
	unzip $^ $(@F) -d $(@D)

.INTERMEDIATE: terminology_download_script.zip
terminology_download_script.zip:
	wget -O $@ http://download.nlm.nih.gov/rxnorm/terminology_download_script.zip

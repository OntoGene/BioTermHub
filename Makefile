WWWDIRS := $(shell python3 -m bth settings -dvr)
LOGFILE := $(shell python3 -m bth settings -vr -n log_file)

init: $(WWWDIRS) $(LOGFILE)

$(WWWDIRS):
	mkdir -p $@ && chmod a+w,g+s $@

$(LOGFILE):
	touch $@ && chmod a+w $@

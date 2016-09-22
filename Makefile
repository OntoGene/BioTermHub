WWWDIRS := $(shell core/settings.py -dvr)
LOGFILE := $(shell core/settings.py -fvr)

init: $(WWWDIRS) $(LOGFILE)

$(WWWDIRS):
	mkdir -p $@ && chmod a+w,g+s $@

$(LOGFILE):
	touch $@ && chmod a+w $@

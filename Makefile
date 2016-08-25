HERE := $(dir $(lastword $(MAKEFILE_LIST)))
WWWDIRS := $(addprefix $(HERE),dumps update/logs www/stats www/batch)
LOGFILE := $(HERE)www/interface.log

init: $(WWWDIRS) $(LOGFILE)

$(WWWDIRS):
	mkdir -p $@ && chmod a+w,g+s $@

$(LOGFILE):
	touch $@ && chmod a+w $@

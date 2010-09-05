gearmandir=$(localerlanglibdir)/$(PACKET)-$(VERSION)/ebin
EBIN_DIR :=../ebin
INCLUDE_DIR :=../include

INCLUDE_FILE = \
	$(INCLUDE_DIR)/gearman.hrl \
	$(INCLUDE_DIR)/gearman_internal.hrl \
	$(INCLUDE_DIR)/gearman_log.hrl

SOURCE_FILE= \
    gearman_app.erl \
    gearman_sup.erl \
    gearman_client_server.erl 

APP_FILE = $(EBIN_DIR)/gearman.app

BEAM_FILE = \
    $(EBIN_DIR)/gearman_app.beam \
    $(EBIN_DIR)/gearman_sup.beam \
    $(EBIN_DIR)/gearman_client.beam \
    $(EBIN_DIR)/gearman_client_server.beam 

BEAM_FILE_NO_DIR = \
	$(notdir $(BEAM_FILE))

BEAM_FILE_IN_APP := $(BEAM_FILE_NO_DIR:%.beam=|%|,)

gearman_DATA = \
	$(BEAM_FILE) \
	$(APP_FILE)

EXTRA_DIST =  \
    $(INCLUDE_FILE) \
    $(SOURCE_FILE) \
	$(BEAM_FILE) \
	$(APP_FILE)

CLEANFILES = \
	$(BEAM_FILE) \
	$(APP_FILE)

ERLCFLAGS += -I ../include

if CTEST
ERLCFLAGS += +debug_info -DCTEST 
else 
ERLCFLAGS += +debug_info 
endif

#if EUNIT
#ERLCFLAGS += -DEUNIT
#endif

$(EBIN_DIR)/%.beam: %.erl $(INCLUDE_FILE)
	@mkdir -p ../ebin
	$(ERLC) $(ERLCFLAGS) -o $(EBIN_DIR) $<

$(EBIN_DIR)/%.app: %.app.in
	@sed -e 's:PACKAGE_NAME:$(PACKAGE_NAME):g' \
		-e 's:PACKAGE_VERSION:$(PACKAGE_VERSION):g' \
		-e 's:MODULES:$(BEAM_FILE_IN_APP):g' \
		-e "s:|:':g" \
		-e 's:,]:]:g' $< > $@


ACLOCAL_AMFLAGS= -I m4
AUTOMAKE_OPTIONS = foreign

bin_PROGRAMS =
sbin_PROGRAMS = 
check_PROGRAMS =
noinst_PROGRAMS =
lib_LTLIBRARIES =
noinst_LTLIBRARIES =
nobase_include_HEADERS =
noinst_HEADERS =
EXTRA_DIST = bootstrap.sh
CLEANFILES =

SUBDIRS = src c_src

if CTEST
comm_test: 
	(mkdir -p ./test/log)
	(erl -s ct_run script_start -DTEST -logdir `pwd`/test/log -include `pwd`/include -pa `pwd`/ebin -cover test/test.coverspec -dir . -s init stop)
endif

INSTALL_GEARMAN_DIR=$(ERLANG_LIB_DIR)/$(PACKAGE_NAME)-$(PACKAGE_VERSION)

install-data-local:
	@echo install the gearman to erlang lib
	@mkdir -p $(INSTALL_GEARMAN_DIR)
	@cp -rf ./ebin ./include ./priv ./src\
		$(INSTALL_GEARMAN_DIR)

uninstall-local:
	@echo uninstall the gearman from erlang lib
	@rm -rf $(INSTALL_GEARMAN_DIR)


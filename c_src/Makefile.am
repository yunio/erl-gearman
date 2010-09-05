privdir = $(top_builddir)/priv
privlibdir = $(privdir)/lib

gearmandrvdir = $(privlibdir)
gearmandrv = $(gearmandrvdir)/gearman_drv.so

#gearmandrv_DATA = $(gearmandrv)
#EXTRA_DIST = $(gearmandrv)
noinst_DATA = $(gearmandrv)
CLEANFILES = $(gearmandrv)

INCLUDES = -I . -I $(ERLANG_ROOT_DIR)/usr/include -I $(ERLANG_LIB_DIR_erl_interface)/include

lib_LTLIBRARIES = libgearman_drv.la
libgearman_drv_la_LDFLAGS = -avoid-version -shared -lgearman
libgearman_drv_la_SOURCES = gearman_drv.c 

$(gearmandrv) : 
	@echo "copy drv to $@"
	@mkdir -p `dirname $@`
	@cp .libs/libgearman_drv.so $@


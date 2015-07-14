EXTRA_DIST = adapter_statistic.cc

lib_LTLIBRARIES = adapter_statistic.la

noinst_HEADERS = \
	sample.h \
	\
	autoconf.h 

ecap_adapter_statistic_la_SOURCES = adapter_statistic.cc
ecap_adapter_statistic_la_LDFLAGS = -module -avoid-version $(libecap_LIBS)

# -shared -export-dynamic -Wl,-soname,ecap_noop_adapter.so

DISTCLEANFILES = \
        autoconf.h

AM_CPPFLAGS = -I$(top_srcdir)/src $(libecap_CFLAGS)


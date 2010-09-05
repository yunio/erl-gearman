#!/bin/sh

aclocal && \
libtoolize --force --copy && \
autoconf -d && \
automake --add-missing --copy --force

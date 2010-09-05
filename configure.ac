#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.63])
AC_INIT([gearman], [0.1], [richie@yun.io])
AC_CONFIG_AUX_DIR(build-aux)
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CC
AC_PROG_MAKE_SET
AC_PROG_LIBTOOL

if test "x$GCC" = "xyes"; then
    CFLAGS="$CFLAGS -Wall"
fi

#locating erlang
AC_ERLANG_NEED_ERLC
AC_ERLANG_NEED_ERL
AC_ERLANG_SUBST_ROOT_DIR
AC_ERLANG_SUBST_LIB_DIR

echo "Erlang Root : $ERLANG_ROOT_DIR"

AC_ERLANG_CHECK_LIB([stdlib],
                    [echo -n "stdlib version \"$ERLANG_LIB_VER_stdlib\"" 
                     echo " is installed in \"$ERLANG_LIB_DIR_stdlib\""],
                     [AC_MSG_ERROR([stdlib was not found!])])
AC_ERLANG_CHECK_LIB([sasl])
AC_ERLANG_CHECK_LIB([crypto])
AC_ERLANG_CHECK_LIB([erl_interface])

#ERLCFLAGS="+debug_info"
AC_ARG_ENABLE([ctest],
[  --enable-ctest   Turn on common test],
[case "${enableval}" in
yes) ctest=true ;;
no)  ctest=false ;;
*) AC_MSG_ERROR([bad value ${enableval} for --enable-ctest]) ;;
esac],[ctest=false])
AM_CONDITIONAL([CTEST], [test x"$ctest" = xtrue])

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

# 添加AM_INIT_AUTOMAKE
AM_INIT_AUTOMAKE([foreign])

AC_CONFIG_FILES(Makefile src/Makefile c_src/Makefile) 
AC_OUTPUT

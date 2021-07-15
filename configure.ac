AC_INIT([sairedis],[1.0])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER(config.h)
AM_INIT_AUTOMAKE([subdir-objects foreign serial-tests])
AC_LANG_C
AC_LANG([C++])
AC_PROG_CC
AC_PROG_CXX
AC_PROG_LIBTOOL
AC_HEADER_STDC
AM_PATH_PYTHON
AM_PATH_PYTHON3

AM_CONDITIONAL(sonic_asic_platform_barefoot,   test x$CONFIGURED_PLATFORM = xbarefoot)
AM_CONDITIONAL(sonic_asic_platform_mellanox,   test x$CONFIGURED_PLATFORM = xmellanox)

AC_ARG_ENABLE(debug,
[  --enable-debug          turn on debugging],
[case "${enableval}" in
	yes) debug=true ;;
	no)  debug=false ;;
	*) AC_MSG_ERROR(bad value ${enableval} for --enable-debug) ;;
esac],[debug=false])
AM_CONDITIONAL(DEBUG, test x$debug = xtrue)

AC_ARG_WITH(sai,
[  --with-sai=vs           compiling against specific SAI],
[case "${withval}" in
	vs)   sai=vs ;;
	*) AC_MSG_ERROR(bad value ${withval} for --with-sai) ;;
esac])
AM_CONDITIONAL(SAIVS,   test x$sai = xvs)

AC_ARG_WITH(swss-common-inc,
[  --with-swss-common-inc=DIR
                           prefix where swss-common includes are installed],
[AC_SUBST(CPPFLAGS, "$CPPFLAGS -I${withval}")
 AC_SUBST(SWSS_COMMON_INC_DIR, "${withval}")])

AC_ARG_WITH(swss-common-lib,
[  --with-swss-common-lib=DIR
                           prefix where swss-common libraries are installed],
[AC_SUBST(LDFLAGS, "$LDFLAGS -L${withval}")
 AC_SUBST(SWSS_COMMON_LIB_DIR, "${withval}")])

AC_ARG_ENABLE(syncd,
[  --enable-syncd          build syncd],
[case "${enableval}" in
   yes) syncd=true ;;
   no)  syncd=false ;;
   *) AC_MSG_ERROR(bad value ${enableval} for --enable-syncd) ;;
esac],[syncd=true])
AS_IF([test "x${ENABLESYNCD}" = "xno" ],[syncd=false],[])
AM_CONDITIONAL(SYNCD, test x$syncd = xtrue)
AM_COND_IF([SYNCD], [
        AM_COND_IF([SAIVS], [], [AC_CHECK_LIB([sai], [main], [AC_MSG_NOTICE(libsai found)], [AC_MSG_ERROR(libsai is required for syncd)])
])])

AC_ARG_ENABLE(rpcserver,
[  --enable-rpcserver      compile with thrift rpcserver included],
[case "${enableval}" in
   yes) rpcserver=true ;;
   no)  rpcserver=false ;;
   *) AC_MSG_ERROR(bad value ${enableval} for --enable-rpcserver) ;;
esac],[rpcserver=false])
AM_CONDITIONAL(SAITHRIFT, test x$rpcserver = xtrue)
AM_COND_IF([SAITHRIFT], [
        AC_CHECK_LIB([thrift], [main], [AC_MSG_NOTICE(libthrift found)], [AC_MSG_ERROR(libthrift is required for rpcserver)])
])

AC_ARG_ENABLE(redis-test,
[  --enable-redis-test     test with redis service],
[case "${enableval}" in
	yes) rtest=true ;;
	no)  rtest=false ;;
	*) AC_MSG_ERROR(bad value ${enableval} for --enable-redis-test) ;;
esac],[rtest=false])
AM_CONDITIONAL(RTEST, test x$rtest = xtrue)

if test x$CONFIGURED_ARCH = xarmhf && test x$CROSS_BUILD_ENVIRON = xy; then
	AM_CONDITIONAL(ARCH64, false)
else
	AM_CONDITIONAL(ARCH64, test `getconf LONG_BIT` = "64")
fi

AC_PATH_PROGS(SWIG, [swig3.0 swig])

CFLAGS_COMMON=""
CFLAGS_COMMON+=" -ansi"
CFLAGS_COMMON+=" -fPIC"
CFLAGS_COMMON+=" -std=c++11"
CFLAGS_COMMON+=" -Wall"
CFLAGS_COMMON+=" -Wcast-align"
CFLAGS_COMMON+=" -Wcast-qual"
CFLAGS_COMMON+=" -Wconversion"
CFLAGS_COMMON+=" -Wdisabled-optimization"
CFLAGS_COMMON+=" -Werror"
CFLAGS_COMMON+=" -Wextra"
CFLAGS_COMMON+=" -Wfloat-equal"
CFLAGS_COMMON+=" -Wformat=2"
CFLAGS_COMMON+=" -Wformat-nonliteral"
CFLAGS_COMMON+=" -Wformat-security"
CFLAGS_COMMON+=" -Wformat-y2k"
CFLAGS_COMMON+=" -Wimport"
CFLAGS_COMMON+=" -Winit-self"
CFLAGS_COMMON+=" -Wno-inline"
CFLAGS_COMMON+=" -Winvalid-pch"
CFLAGS_COMMON+=" -Wmissing-field-initializers"
CFLAGS_COMMON+=" -Wmissing-format-attribute"
CFLAGS_COMMON+=" -Wmissing-include-dirs"
CFLAGS_COMMON+=" -Wmissing-noreturn"
CFLAGS_COMMON+=" -Wno-aggregate-return"
CFLAGS_COMMON+=" -Wno-padded"
CFLAGS_COMMON+=" -Wno-switch-enum"
CFLAGS_COMMON+=" -Wno-unused-parameter"
CFLAGS_COMMON+=" -Wpacked"
CFLAGS_COMMON+=" -Wpointer-arith"
CFLAGS_COMMON+=" -Wredundant-decls"
CFLAGS_COMMON+=" -Wshadow"
CFLAGS_COMMON+=" -Wstack-protector"
CFLAGS_COMMON+=" -Wstrict-aliasing=3"
CFLAGS_COMMON+=" -Wswitch"
CFLAGS_COMMON+=" -Wswitch-default"
CFLAGS_COMMON+=" -Wunreachable-code"
CFLAGS_COMMON+=" -Wunused"
CFLAGS_COMMON+=" -Wvariadic-macros"
CFLAGS_COMMON+=" -Wwrite-strings"
CFLAGS_COMMON+=" -Wno-switch-default"
CFLAGS_COMMON+=" -Wconversion"
CFLAGS_COMMON+=" -Wno-psabi"

SAVED_FLAGS="$CXXFLAGS"
CXXFLAGS="-Wcast-align=strict"
AC_MSG_CHECKING([whether CXX supports -Wcast-align=strict])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([])],
    [AC_MSG_RESULT([yes])]
    [AC_SUBST([CFLAGS_COMMON], ["${CFLAGS_COMMON} -Wcast-align=strict"])],
    [AC_MSG_RESULT([no])])
CXXFLAGS="$SAVED_FLAGS"

AC_SUBST(NO_CAST_FUNCTION_TYPE)

SAVED_FLAGS="$CXXFLAGS"
CXXFLAGS="-Wno-cast-function-type"
AC_MSG_CHECKING([whether CXX supports -Wno-cast-function-type])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([])],
    [AC_MSG_RESULT([yes])]
    [AC_SUBST([NO_CAST_FUNCTION_TYPE], ["-Wno-cast-function-type"])],
    [AC_MSG_RESULT([no])])
CXXFLAGS="$SAVED_FLAGS"

AC_SUBST(CFLAGS_COMMON)

AC_OUTPUT(Makefile
          meta/Makefile
          lib/Makefile
          lib/src/Makefile
          vslib/Makefile
          vslib/src/Makefile
          syncd/Makefile
          saiplayer/Makefile
          saidump/Makefile
          saisdkdump/Makefile
          saidiscovery/Makefile
          saiasiccmp/Makefile
          tests/Makefile
          pyext/Makefile
          pyext/py2/Makefile
          pyext/py3/Makefile
          python/setup.py)

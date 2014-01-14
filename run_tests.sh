#!/bin/bash

# Runs the tests for a given build type. Creates a CTestResults.xml
# file in the xUint format.
#
# Usage: ./run_tests.sh [<build_type>]
#
# e.g. ./run_tests.sh Debug

CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${CURDIR}

if [ $# -eq 0 ] ; then
    BUILDTYPE=Debug
else
    BUILDTYPE=$1
fi

BUILDDIR=build/${BUILDTYPE}
if [ ! -f ${BUILDDIR} ] ; then
    make BUILD_TYPE=${BUILDTYPE}
    if [ $? -ne 0 ] ; then
        exit 1
    fi
fi
    
cd ${BUILDDIR}

ctest -T test --no-compress-output || true
if [ -f Testing/TAG ] ; then
   xsltproc ${CURDIR}/test_utils/ctest2junix.xsl Testing/`head -n 1 < Testing/TAG`/Test.xml > ${CURDIR}/CTestResults.xml

fi

cd ${CURDIR}
nosetests --with-coverage --with-xunit --cover-package . --cover-erase --exe --cover-xml || true
#!/bin/bash
#
# This script compiles a avdl schema definition into the JSON avsc
# file needed by Avro. We only keep the avsc file for the record that
# is the same name as the avdl filename. e.g. In the
# TrackerEvent.avdl, we will only grab the record called TrackerEvent.
#
# You should never edit the avsc files directly and instead use this
# script to make them.
#
# Usage: ./compile_schema.sh <path-to-avdl> [<output-dir>]
#
# Author: Mark Desnoyer (desnoyer@neon-labs.com)
# Copyright 2014 Neon Labs Inc.

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Usage: `basename $0` <path-to-avdl> [<output-dir>]"
    exit 1
fi

outputDir=$2
if [ ! -n "$2" ]; then
    outputDir=`dirname $1`
fi

mkdir -p ${outputDir}

extractionDir=`mktemp -d`

# Compile the ivdl file
java -jar /usr/lib/avro/avro-tools.jar idl2schemata $1 ${extractionDir}

# Move the compiled avsc file to its final resting place
avscFile=`basename $1 | sed 's/\.avdl/\.avsc/g'`
mv ${extractionDir}/${avscFile} ${outputDir}

# Delete the temporary directory
rm -rf $extractionDir
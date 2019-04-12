#!/usr/bin/env bash

set -x
JDIR=$(dirname $0)/../
java  -classpath "$JDIR/target/*:$JDIR/target/lib/*"  ${EXECMAIN:=com.fnreport.BatchBuild} "$1" "$2" "$3" "$4" "$5"

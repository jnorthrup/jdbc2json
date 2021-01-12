#!/usr/bin/env bash

set -fx
JDIR=$(dirname $0)/../
exec java -Duser=${USER} -Dpassword=${PASSWORD}  -classpath "$JDIR/target/*:$JDIR/target/lib/*"  ${EXECMAIN:=com.fnreport.JdbcToCouchDbBulkKt} "$@"

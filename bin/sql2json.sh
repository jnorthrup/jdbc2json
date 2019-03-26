#!/usr/bin/env bash

#HAZELCAST_SUPPORT=( --add-modules java.se
#    {--add-exports' 'java.base/jdk.internal.ref,--add-opens' '{java.{base/{java.{lang,nio},sun.nio.ch},management/sun.management},jdk.management/com.sun.management.internal}}=ALL-UNNAMED
#    )
set -x
JDIR=$(dirname $0)/../
java  ${HAZELCAST_SUPPORT[@]} -classpath "$JDIR./target/*:$JDIR./target/lib/*"  ${EXECMAIN:=com.fnreport.BatchBuild} $@

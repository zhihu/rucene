#!/bin/sh
PRG="$0"
progname=`basename "$0"`
while [ -h "$PRG" ] ; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
  PRG="$link"
  else
  PRG=`dirname "$PRG"`"/$link"
  fi
done
RUCENE_HOME=`dirname "$PRG"`/..
RUCENE_HOME=`cd "$RUCENE_HOME" > /dev/null && pwd`

if [ -z "$RUCENE_DEBUG" ] ; then
  cargo build --release
else
  cargo build
fi

. $RUCENE_HOME/scripts/common.sh

cat $QUERY_PATH | $RUCENE_TARGET_HOME/benchmark $INDEX_PATH $FIELD

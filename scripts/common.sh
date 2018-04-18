#!/bin/sh

[ -z "$INDEX_PATH" ] && export INDEX_PATH="$RUCENE_HOME/index"
[ -z "$FIELD" ] && FIELD="content"
[ -z "$QUERY_PATH" ] && export QUERY_PATH="$RUCENE_HOME/queries.txt"

if [ ! -f $QUERY_PATH ]; then
  echo "Queries input file '$QUERY_PATH' does not exist"
  exit 1
fi

if [ ! -d $INDEX_PATH ]; then
  echo "Index path '$INDEX_PATH' does not exist"
  exit 1
fi

if [ ! -f $RUCENE_HOME/target ]; then
  # rucene is being used within a workspace
  RUCENE_TARGET_HOME=`dirname "$RUCENE_HOME"`/..
  RUCENE_TARGET_HOME=`cd "$RUCENE_HOME" > /dev/null && pwd`
else
  RUCENE_TARGET_HOME=$RUCENE_HOME
fi

if [ -z "$RUCENE_DEBUG" ] ; then
  RUCENE_TARGET_HOME=$RUCENE_TARGET_HOME/target/release
else
  RUCENE_TARGET_HOME=$RUCENE_TARGET_HOME/target/debug
fi
export RUCENE_TARGET_HOME

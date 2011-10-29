#! /usr/bin/env bash
#
# hdfs-file-slurper.sh:  copy files from local disk into HDFS
#

# get the current directory
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "HADOOP_CONF_DIR must be defined and refer to your Hadoop config directory"
  exit 2;
fi

CLASSPATH="${HADOOP_CONF_DIR}:$bin/../dist/lib/*:$bin/../lib/*"

# add classes first, triggers log4j.properties priority
if [ -d "${bin}/../target/classes" ]; then
  CLASSPATH=${CLASSPATH}:${bin}/../target/classes
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx512m

"$JAVA" $JAVA_HEAP_MAX -classpath "$CLASSPATH" com.alexholmes.hdfsslurper.Slurper "$@"
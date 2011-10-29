#! /usr/bin/env bash
#
# hdfs-file-slurper.sh:  copy files from local disk into HDFS
#

# get the current directory
script=`basename "$0"`
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

pidfile=$bin/$script.pid

if [ -f "$pidfile" ]; then
  pid=`cat $pidfile`
  if [ -d "/proc/$pid" ]; then
    echo "PID file $pidfile exists and PID $pid is running"
    exit 1
  fi
  echo "Pid file $pidfile exists but PID $pid no longer seems to be running, ignoring PID file"
fi


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

"$JAVA" $JAVA_HEAP_MAX -classpath "$CLASSPATH" com.alexholmes.hdfsslurper.Slurper "$@" &
pid=$!
echo $pid > $pidfile

wait $pid

exitCode=$?

if [ -f "$pidfile" ]; then
  rm $pidfile
fi

exit $exitCode
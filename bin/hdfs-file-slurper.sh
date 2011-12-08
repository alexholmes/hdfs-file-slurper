#! /usr/bin/env bash
#
# hdfs-file-slurper.sh:  copy files from local disk into HDFS
#

# get the current directory
script=`basename "$0"`
bin=`dirname "$0"`
SLURPER_HOME=`cd "$bin/../">/dev/null; pwd`

pidfile=$bin/$script.pid

if [ -f "$pidfile" ]; then
  pid=`cat $pidfile`
  if [ -d "/proc/$pid" ]; then
    echo "PID file $pidfile exists and PID $pid is running"
    exit 1
  fi
  echo "Pid file $pidfile exists but PID $pid no longer seems to be running, ignoring PID file"
fi

if [ ! -f "$HADOOP_BIN" ]; then
  if [ -f "/usr/bin/hadoop" ]; then
    HADOOP_BIN="/usr/bin/hadoop"
    echo "HADOOP_BIN environment not set, but found script under $HADOOP_BIN"
  else
    echo "HADOOP_BIN must be set and point to the hadoop script"
    echo "If hadoop is already in the path then this is as simple as export HADOOP_BIN=`which hadoop`"
    exit 2;
  fi
fi

SLURPER_JAR_DIR=$SLURPER_HOME/lib

function add_to_classpath() {
  dir=$1
  for f in $dir/*.jar; do
    SLURPER_CLASSPATH=${SLURPER_CLASSPATH}:$f;
  done

  export SLURPER_CLASSPATH
}

add_to_classpath ${SLURPER_JAR_DIR}

export SLURPER_CLASSPATH=$SLURPER_HOME/conf:$SLURPER_CLASSPATH

export HADOOP_CLASSPATH="${SLURPER_CLASSPATH}:${HADOOP_CLASSPATH}"

"$HADOOP_BIN" jar ${bin}/../dist/lib/* com.alexholmes.hdfsslurper.Slurper  "$@"
#"$HADOOP_BIN" jar ${bin}/../dist/lib/* com.alexholmes.hdfsslurper.Slurper  "$@" &


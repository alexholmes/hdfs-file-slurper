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

if [ -z "$HADOOP_BIN" ]; then
  echo "HADOOP_BIN must be set"
  exit 2;
fi

"$HADOOP_BIN" jar ${bin}/../dist/lib/* com.alexholmes.hdfsslurper.Slurper  "$@" &

pid=$!
echo $pid > $pidfile

wait $pid

exitCode=$?

if [ -f "$pidfile" ]; then
  rm $pidfile
fi

exit $exitCode
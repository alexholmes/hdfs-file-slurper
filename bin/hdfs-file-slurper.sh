#! /usr/bin/env bash
#
# hdfs-file-slurper.sh:  copy files between local disk and HDFS
#

# get the current directory
script=`basename "$0"`
bin=`dirname "$0"`
SLURPER_HOME=`cd "$bin/../">/dev/null; pwd`

pidfile=$bin/$script.pid

if [ -f "$pidfile" ]; then
  pid=`cat $pidfile`
  if [ -d "/proc/$pid" ]; then
    echo "PID file $pidfile exists and PID $pid is still running"
    exit 1
  fi
  echo "Pid file $pidfile exists but PID $pid no longer seems to be running, ignoring PID file"
fi

CDH_HADOOP_HOME=/usr/lib/hadoop

if [ ! -d "${HADOOP_HOME}" ]; then
  if [ -d "${CDH_HADOOP_HOME}" ]; then
    HADOOP_HOME=${CDH_HADOOP_HOME}
    echo "HADOOP_HOME environment not set, but found ${HADOOP_HOME} in path so using that"
  else
    echo "HADOOP_HOME must be set and point to the hadoop home directory"
    exit 2;
  fi
fi

HADOOP_CONF_DIR=${HADOOP_HOME}/conf

SLURPER_JAR_DIR=$SLURPER_HOME/lib

function add_to_classpath() {
  dir=$1
  for f in $dir/*.jar; do
    SLURPER_CLASSPATH=${SLURPER_CLASSPATH}:$f;
  done

  export SLURPER_CLASSPATH
}

add_to_classpath ${SLURPER_JAR_DIR}

function add_to_hadoop_classpath() {
  dir=$1
  for f in $dir/*.jar; do
    HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f;
  done

  export HADOOP_CLASSPATH
}

HADOOP_LIB_DIR=$HADOOP_HOME
add_to_hadoop_classpath ${HADOOP_LIB_DIR}
HADOOP_LIB_DIR=$HADOOP_HOME/lib
add_to_hadoop_classpath ${HADOOP_LIB_DIR}

SLURPER_JAR="${SLURPER_HOME}/dist/lib/*"

export CLASSPATH=${SLURPER_HOME}/conf:${SLURPER_JAR}:${HADOOP_CONF_DIR}:${HADOOP_CLASSPATH}:${SLURPER_CLASSPATH}

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx512m


echo ${HADOOP_HOME}

if [ -d "${HADOOP_HOME}/build/native" -o -d "${HADOOP_HOME}/lib/native" -o -d "${HADOOP_HOME}/sbin" ]; then
  JAVA_PLATFORM=`CLASSPATH=${CLASSPATH} ${JAVA} -Xmx32m ${HADOOP_JAVA_PLATFORM_OPTS} org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`

  if [ -d "$HADOOP_HOME/build/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
        JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/build/native/${JAVA_PLATFORM}/lib
    else
        JAVA_LIBRARY_PATH=${HADOOP_HOME}/build/native/${JAVA_PLATFORM}/lib
    fi
  fi

  if [ -d "${HADOOP_HOME}/lib/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
      JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}
    else
      JAVA_LIBRARY_PATH=${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}
    fi
  fi
fi


date=`date +"%Y%m%d-%H%M%S"`
outfile=$SLURPER_HOME/logs/slurper-$date.out
errfile=$SLURPER_HOME/logs/slurper-$date.err


nohup "$JAVA" $JAVA_HEAP_MAX -Djava.library.path=${JAVA_LIBRARY_PATH} -classpath "$CLASSPATH" com.alexholmes.hdfsslurper.Slurper "$@" > $outfile 2> $errfile < /dev/null &

pid=$!

echo $pid > $pidfile

echo "Slurper process running as pid ${pid}"

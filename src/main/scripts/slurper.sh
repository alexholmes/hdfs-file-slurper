#! /usr/bin/env bash
##########################################################################
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
##########################################################################
#
# slurper.sh:  Copy files between local disk and HDFS.
#
# Supports running in daemon and non-daemon mode (non-daemon is
# the default).
#
##########################################################################

mode=normal

# go through args and determine if we're being launched
# in daemon mode
#
for i in $*; do
  if [ "--daemon" = "$i" ]; then
    mode=daemon
  elif [ "--daemon-no-bkgrnd" = "$i" ]; then
    mode=daemon-no-bkgrnd
  fi
done

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`
SCRIPT=`basename ${PRG}`

cd $BASEDIR

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

SLURPER_JAR_DIR=$BASEDIR/lib

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

SLURPER_JAR="${BASEDIR}/dist/lib/*"

export CLASSPATH=${SLURPER_JAR}:${HADOOP_CONF_DIR}:${HADOOP_CLASSPATH}:${SLURPER_CLASSPATH}

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx512m

# pick up the native Hadoop directory if it exists
# this is to support native compression codecs
 #
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
outfile=$BASEDIR/logs/slurper-$date.out
errfile=$BASEDIR/logs/slurper-$date.err

case "$mode" in
  normal)
    export CLASSPATH=${BASEDIR}/conf/normal:${CLASSPATH}
    "$JAVA" $JAVA_HEAP_MAX -Djava.library.path=${JAVA_LIBRARY_PATH} -classpath "$CLASSPATH" com.alexholmes.hdfsslurper.Slurper "$@"
  ;;
  daemon-no-bkgrnd)
    export CLASSPATH=${BASEDIR}/conf/daemon:${CLASSPATH}
    nohup "$JAVA" $JAVA_HEAP_MAX -Djava.library.path=${JAVA_LIBRARY_PATH} -classpath "$CLASSPATH" com.alexholmes.hdfsslurper.Slurper "$@" > $outfile 2> $errfile < /dev/null
  ;;
  daemon)
    pidfile=$BASEDIR/$SCRIPT.pid

    if [ -f "$pidfile" ]; then
      pid=`cat $pidfile`
      if [ -d "/proc/$pid" ]; then
        echo "PID file $pidfile exists and PID $pid is still running"
        exit 1
      fi
      echo "Pid file $pidfile exists but PID $pid no longer seems to be running, ignoring PID file"
    fi

    export CLASSPATH=${BASEDIR}/conf/daemon:${CLASSPATH}
    nohup "$JAVA" $JAVA_HEAP_MAX -Djava.library.path=${JAVA_LIBRARY_PATH} -classpath "$CLASSPATH" com.alexholmes.hdfsslurper.Slurper "$@" > $outfile 2> $errfile < /dev/null &
    pid=$!

    echo $pid > $pidfile

    echo "Slurper daemon running as pid ${pid}"
  ;;
  *)
    echo "Unexpected execution mode: '$mode'"
    exit 1
  ;;
esac


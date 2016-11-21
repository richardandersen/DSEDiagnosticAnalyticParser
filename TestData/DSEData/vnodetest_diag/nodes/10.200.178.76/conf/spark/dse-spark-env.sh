#!/bin/sh

log_config() {
    result="-Dlogback.configurationFile=$1 -Dcassandra.logdir=$CASSANDRA_LOG_DIR"
    if [ "$2" != "" ] && [ "$3" != "" ]; then
        result="$result -Dspark.log.dir=$2 -Dspark.log.file=$3"
    fi
    echo "$result"
}

# Library paths... not sure whether they are required for
# TODO consider using LD_LIBRARY_PATH or DYLD_LIBRARY_PATH env variables
SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH  -Dcassandra.logdir=$CASSANDRA_LOG_DIR"

# Memory options
export SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -XX:MaxHeapFreeRatio=50 -XX:MinHeapFreeRatio=20"  # don't use too much memory

# Set library paths for Spark daemon process as well as to be inherited by executor processes
if [ "$(echo "$OSTYPE" | grep "^darwin")" != "" ]; then
    # For MacOS...
    export DYLD_LIBRARY_PATH="$HADOOP2_JAVA_LIBRARY_PATH"
else
    # For any other Linux-like OS
    export LD_LIBRARY_PATH="$HADOOP2_JAVA_LIBRARY_PATH"
fi

export SPARK_SERVER_LOGBACK_CONF_FILE="$SPARK_CONF_DIR/logback-spark-server.xml"

SPARK_EXECUTOR_LOGBACK_CONF_FILE="$SPARK_CONF_DIR/logback-spark-executor.xml"
SPARK_SUBMIT_LOGBACK_CONF_FILE="${SPARK_SUBMIT_LOGBACK_CONF_FILE:-"$SPARK_CONF_DIR/logback-spark.xml"}"

# spark.kryoserializer.buffer.mb has been removed since it is deprecated in Spark 1.4 and we actually do
# not use Kryo by default.
export SPARK_COMMON_OPTS="$DSE_OPTS "

export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS  -Djava.library.path=$HADOOP2_JAVA_LIBRARY_PATH"
export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS $SPARK_COMMON_OPTS "
export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS $(log_config $SPARK_EXECUTOR_LOGBACK_CONF_FILE) "
export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS -Ddse.client.configuration.impl=com.datastax.bdp.transport.client.HadoopBasedClientConfiguration "
export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS -Dspark.master.autoUpdate=false "
export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS -Dspark.cassandra.connection.host.autoUpdate=false "
export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS -Dspark.hadoop.cassandra.host.autoUpdate=false "
export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS -Dspark.hadoop.fs.default.name.autoUpdate=false "
export LOCAL_SPARK_EXECUTOR_OPTS="$LOCAL_SPARK_EXECUTOR_OPTS -Dspark.hadoop.fs.defaultFS.autoUpdate=false "

export LOCAL_SPARK_DRIVER_OPTS="$LOCAL_SPARK_DRIVER_OPTS $SPARK_COMMON_OPTS "
export LOCAL_SPARK_DRIVER_OPTS="$LOCAL_SPARK_DRIVER_OPTS $(log_config $SPARK_SUBMIT_LOGBACK_CONF_FILE) "
export LOCAL_SPARK_DRIVER_OPTS="$LOCAL_SPARK_DRIVER_OPTS -Ddse.client.configuration.impl=com.datastax.bdp.transport.client.HadoopBasedClientConfiguration "
export LOCAL_SPARK_DRIVER_OPTS="$LOCAL_SPARK_DRIVER_OPTS -Dderby.stream.error.method=com.datastax.bdp.derby.LogbackBridge.getLogger "

export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS $LOCAL_SPARK_DRIVER_OPTS $SPARK_DRIVER_OPTS "

export HWI_WAR_FILE="$(find "$SPARK_HOME"/lib -name 'hive-hwi-*.jar')"

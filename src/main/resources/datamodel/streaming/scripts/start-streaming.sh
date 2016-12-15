#! /bin/bash

if [ -z $HOTSTAR_HOME ]
then
    echo "Error: HOTSTAR_HOME is not set in the environment"
    exit -1
fi


SPARK_APP_HOME=$HOTSTAR_HOME/spark

source $SPARK_APP_HOME/config/streaming.properties

ENV=${ENV:-OSX}
APP_JARS=""
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-3g}
SPARK_EXECUTOR_INSTANCES=${SPARK_EXECUTOR_INSTANCES:-3}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-6656m}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-4}
DRIVER_CLASS=com.hotstar.datamodel.streaming.spark.driver.SparkProcessorDriver

cmd="$1"
if [ -z $cmd ]
then
    cmd="start"
fi

if [ $ENV == "OSX" ]
then
    JOB_ID_COMMAND="ps -ef | grep -v grep | grep -i $DRIVER_CLASS | awk '{print \$2}'"
    KILL_JOB_COMMAND="kill -9 "

elif [ $ENV == "APACHE" ]
then
    JOB_ID_COMMAND="hadoop job -list | grep -i $DRIVER_CLASS |  awk '{print \$1}'"
    KILL_JOB_COMMAND="hadoop job -kill "

elif [ $ENV == "CLOUDERA" ]
then
    JOB_ID_COMMAND="yarn application -list | grep -i $DRIVER_CLASS | awk '{print \$1}'"
    KILL_JOB_COMMAND="yarn application -kill "

else
    echo -e "\033[31m[ERROR]\033[m Environment '$ENV' is not supported"
    exit -1
fi

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
if [ $cmd = "start" ]
then
    jobid=$(eval $JOB_ID_COMMAND)
    if [ ! -z "$jobid" ]
    then
        echo -e "\033[32m[INFO]\033[m Streaming already running with jobid: $jobid"
    else
        for jar in `find $SPARK_APP_HOME/lib/ -name "*.jar" -not -name "streaming*.jar"`;
        do
            if [[ -z $APP_JARS ]]; then
                APP_JARS="$jar"
            else
                APP_JARS="$APP_JARS,$jar"
            fi
        done

        for jar in `find $SPARK_APP_HOME/lib/ -name "*.jar" -not -name "streaming*.jar"`;
        do
            if [[ -z $STREAMING_JARS ]]; then
                STREAMING_JARS="$jar"
            else
                STREAMING_JARS="$STREAMING_JARS:$jar"
        fi
        done

        #To enable rdd compression and kryo serialization
        SPARK_SUBMIT_PARAMS="--conf spark.rdd.compress=true"

        #Number of milliseconds to wait to launch a data-local task before giving up and launching it on a less-local node
        SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --conf spark.locality.wait=60000"

        if [ $ENV == "OSX" ]; then
            SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --master local[*]"

        elif [ $ENV == "BIGINSIGHT" ]; then
            SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --master spark://localhost:7077 --deploy-mode cluster"
            SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --properties-file $SPARK_APP_HOME/resources/streaming/spark-biginsight.properties"
            SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --executor-memory $SPARK_EXECUTOR_MEMORY --driver-memory $SPARK_DRIVER_MEMORY"

        elif [ $ENV == "CLOUDERA" ]; then
            SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --master yarn --deploy-mode cluster"
            SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --executor-memory $SPARK_EXECUTOR_MEMORY --num-executors $SPARK_EXECUTOR_INSTANCES --executor-cores $SPARK_EXECUTOR_CORES"
            SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --properties-file $SPARK_APP_HOME/resources/streaming/spark-yarn.properties"
            SPARK_SUBMIT_PARAMS="$SPARK_SUBMIT_PARAMS --driver-memory $SPARK_DRIVER_MEMORY --conf spark.yarn.executor.memoryOverhead=1536"
			APP_JARS = $APP_JARS,/opt/cloudera/parcels/CDH-5.5.1-1.cdh5.5.1.p0.11/lib/pig/pig-0.12.0-cdh5.5.1-withouthadoop.jar --conf spark.driver.extraClassPath=$STREAMING_JARS:/opt/cloudera/parcels/CDH-5.5.1-1.cdh5.5.1.p0.11/lib/pig/pig-0.12.0-cdh5.5.1-withouthadoop.jar --conf spark.executor.extraClassPath=$STREAMING_JARS:/opt/cloudera/parcels/CDH-5.5.1-1.cdh5.5.1.p0.11/lib/pig/pig-0.12.0-cdh5.5.1-withouthadoop.jar \
        else
            echo -e "\033[31m[ERROR]\033[m Environment '$ENV' is not supported"
            exit -1
        fi

        spark-submit --class $DRIVER_CLASS \
                             $SPARK_SUBMIT_PARAMS \
                             --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.0.2 \
                            $SPARK_APP_HOME/lib/sparkProcessor-2.0.2.jar &
        exit $?
    fi

elif [ $cmd = "stop" ]
then
    echo -e "\033[32m[INFO]\033[m Stopping streaming"
    jobid=$(eval $JOB_ID_COMMAND)
    if [ ! -z "$jobid" ]
    then
        output=$(eval $KILL_JOB_COMMAND $jobid)
        status=$?
        echo -e "\033[32m[INFO]\033[m Stopping streaming output: $output"
        exit $status
    fi

elif [ $cmd = "status" ]
then
    jobid=$(eval $JOB_ID_COMMAND)
    if [ ! -z "$jobid" ]
    then
        echo -e "\033[32m[INFO]\033[m Streaming running with jobid: $jobid"
    else
        echo -e "\033[32m[INFO]\033[m Streaming not running"
        exit 1
    fi
fi
#! /bin/bash

if [ -z $HOTSTAR_HOME ]
then
    echo "Error: HOTSTAR_HOME is not set in the environment"
    exit -1
fi


rm $HOTSTAR_HOME/spark/config/*
rm $HOTSTAR_HOME/spark/scripts/*
cp -r /Users/shivaa/git/SparkProcessorKinesis_git/src/main/resources/datamodel/streaming/scripts/* $HOTSTAR_HOME/spark/scripts/
cp -r /Users/shivaa/git/SparkProcessorKinesis_git/src/main/resources/datamodel/streaming/config/* $HOTSTAR_HOME/spark/config/
rm $HOTSTAR_HOME/spark/lib/*.jar
cp /Users/shivaa/git/SparkProcessorKinesis_git/target/sparkProcessor-2.0.2.jar $HOTSTAR_HOME/spark/lib/ 

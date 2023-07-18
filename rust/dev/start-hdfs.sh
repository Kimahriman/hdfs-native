#!/bin/bash

if ! stat /opt/hadoop/share/hadoop/tools/lib/mockito-core-*.jar
then
    wget https://repo1.maven.org/maven2/org/mockito/mockito-core/2.28.2/mockito-core-2.28.2.jar -O /opt/hadoop/share/hadoop/tools/lib/mockito-core-2.28.2.jar
fi

export HADOOP_CLASSPATH=$(hadoop classpath):/opt/hadoop/share/hadoop/tools/lib/*

exec hadoop jar share/hadoop/hdfs/hadoop-hdfs-3.3.5-tests.jar org.apache.hadoop.test.MiniDFSClusterManager -format -nnport 9000
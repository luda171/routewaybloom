#!/bin/bash
THE_CLASSPATH=/data2/bloom-app/target/bloom-app-1.0-SNAPSHOT-jar-with-dependencies.jar
export JAVA_HOME=/usr/lib/jvm/java-1.8.0/

/usr/lib/jvm/jre-1.8.0/bin/java  -classpath $THE_CLASSPATH  gov.lanl.bloom.memory.one.BloomWriter  -dirtounload  /data2/haw/ -cdxfile /data2/haw/harvest-2020-index.cdx  -filtername haw2020 -numberofelements 200000000 -probability 0.01  -numberofhashes 5   -redis true

echo 

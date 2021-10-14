#!/bin/bash
THE_CLASSPATH=/data2/bloom-app/target/bloom-app-1.0-SNAPSHOT-jar-with-dependencies.jar
export JAVA_HOME=/usr/lib/jvm/java-1.8.0/

/usr/lib/jvm/jre-1.8.0/bin/java  -classpath $THE_CLASSPATH  gov.lanl.bloom.utils.MultipleBloomLoader  -dirfilename dirs.txt -redishost localhost -redisport 6379 

echo 
